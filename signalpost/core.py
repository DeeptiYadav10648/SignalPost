"""Official Brønnøysund Enhetsregisteret client, evidence model and persistence."""
from __future__ import annotations
import argparse, hashlib, json, logging, re, sqlite3, sys, threading, time
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

BASE = "https://data.brreg.no/enhetsregisteret/api"
LOG = logging.getLogger("signalpost")

class BrregRequestError(RuntimeError):
    def __init__(self, message, status=None):
        super().__init__(message)
        self.status = status

def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

def normalize_org_number(value: str) -> str:
    digits = re.sub(r"[ .]", "", str(value).strip())
    if not re.fullmatch(r"\d{9}", digits):
        raise ValueError("Organization number must contain exactly 9 digits")
    weights = (3, 2, 7, 6, 5, 4, 3, 2)
    check = 11 - sum(int(d) * w for d, w in zip(digits[:8], weights)) % 11
    check = 0 if check == 11 else check
    if check == 10 or check != int(digits[8]):
        raise ValueError("Invalid Norwegian organization number checksum")
    return digits

def fact(value, source_url, retrieved_at, source_updated_at=None, verified=True, derived=False):
    return {"value": value, "source_url": source_url, "retrieved_at": retrieved_at,
            "source_updated_at": source_updated_at, "verified": bool(verified), "derived": bool(derived)}

def validate_evidence(item: dict) -> bool:
    if not isinstance(item, dict) or not {"value", "source_url", "retrieved_at", "source_updated_at", "verified"}.issubset(item): return False
    if not isinstance(item["source_url"], str) or not item["source_url"].startswith("https://") or not isinstance(item["verified"],bool): return False
    try:
        retrieved=datetime.fromisoformat(item["retrieved_at"].replace("Z", "+00:00"))
        if retrieved.tzinfo is None or retrieved.utcoffset().total_seconds() != 0: return False
        if item["source_updated_at"] is not None: datetime.fromisoformat(item["source_updated_at"].replace("Z", "+00:00"))
    except (TypeError, ValueError): return False
    return True

def validate_profile(profile: dict) -> list[str]:
    errors=[]
    try: org=normalize_org_number(profile.get("org_number", ""))
    except ValueError as e: return [str(e)]
    if org != profile.get("org_number"): errors.append("organization number is not normalized")
    facts=profile.get("facts")
    if not isinstance(facts, dict) or not facts: errors.append("facts must be a non-empty object")
    else:
        for key, item in facts.items():
            if not validate_evidence(item): errors.append(f"invalid evidence for {key}")
            if isinstance(item, dict) and item.get("source_url", "").startswith(BASE) and isinstance(item.get("value"), (str,int)) and key == "org_number" and str(item["value"]) != org: errors.append("fact organization identity mismatch")
    try:
        stamp=datetime.fromisoformat(profile.get("retrieved_at", "").replace("Z", "+00:00"))
        if stamp.tzinfo is None or stamp.utcoffset().total_seconds() != 0: errors.append("profile retrieval timestamp must be UTC")
    except (TypeError, ValueError): errors.append("invalid profile retrieval timestamp")
    return errors

class BrregClient:
    def __init__(self, max_requests=1800, timeout=15, retries=2, delay=.15, opener=None):
        self.max_requests=max_requests; self.requests=0; self.timeout=timeout; self.retries=retries; self.delay=delay; self.opener=opener or urlopen
        self._request_lock=threading.Lock(); self._last_request=0.0; self.last_content_sha256=None
    def get(self, path, params=None):
        url=BASE+path+("?"+urlencode(params, doseq=True) if params else "")
        last=None
        for attempt in range(self.retries+1):
            with self._request_lock:
                if self.requests >= self.max_requests: raise BrregRequestError("request budget exhausted")
                self.requests+=1
                wait=self.delay-(time.monotonic()-self._last_request)
                if wait>0: time.sleep(wait)
                self._last_request=time.monotonic()
            try:
                req=Request(url, headers={"Accept":"application/json", "User-Agent":"Signalpost/1.0 (public-data research)"})
                with self.opener(req, timeout=self.timeout) as response:
                    raw=response.read()
                data=json.loads(raw.decode("utf-8"))
                self.last_content_sha256=hashlib.sha256(raw).hexdigest()
                return data, url
            except HTTPError as e:
                last=e
                if e.code in (404,410): return None, url
                if e.code in (401,403): raise BrregRequestError(f"Brønnøysund blocked request (HTTP {e.code})",e.code) from e
                if e.code not in (429, 500, 502, 503, 504) or attempt == self.retries: break
            except (URLError, TimeoutError, json.JSONDecodeError, UnicodeDecodeError) as e:
                last=e
                if attempt == self.retries: break
            time.sleep(min(2 ** attempt, 8))
        status=last.code if isinstance(last,HTTPError) else None
        raise BrregRequestError(f"Brønnøysund request failed: {last}",status) from last
    def lookup(self, org_number):
        org=normalize_org_number(org_number)
        raw,url=self.get("/enheter/"+org)
        if raw is None: return None
        returned=str(raw.get("organisasjonsnummer", ""))
        if returned != org: raise ValueError(f"identity mismatch: requested {org}, received {returned!r}")
        return extract_profile(raw, url, content_sha256=self.last_content_sha256)
    def discover(self, target, start_page=0, page_size=100, on_page=None):
        seen=set(); page=start_page
        while len(seen)<target:
            raw,url=self.get("/enheter", {"page":page,"size":min(page_size,target-len(seen)),"sort":"organisasjonsnummer,ASC"})
            if not isinstance(raw,dict): break
            content=raw.get("_embedded",{}).get("enheter",[])
            if not content: break
            for entry in content:
                org=entry.get("organisasjonsnummer")
                try: org=normalize_org_number(org)
                except (TypeError,ValueError): continue
                if org in seen: continue
                seen.add(org)
                profile=extract_profile(entry, entry.get("_links",{}).get("self",{}).get("href",url), org)
                yield profile
                if len(seen)>=target: break
            if on_page: on_page(page+1)
            page+=1
            if page >= (raw.get("page",{}).get("totalPages",page+1)): break

def extract_profile(raw, source_url, expected_org=None, content_sha256=None):
    org=normalize_org_number(raw.get("organisasjonsnummer", ""))
    if expected_org and org != expected_org: raise ValueError("identity mismatch")
    retrieved=utcnow(); updated=raw.get("oppdateringsdato")
    fields={"org_number":"organisasjonsnummer", "name":"navn", "organization_form":"organisasjonsform",
            "registration_date":"registreringsdatoEnhetsregisteret", "business_address":"forretningsadresse",
            "postal_address":"postadresse", "industry_code_1":"naeringskode1", "industry_code_2":"naeringskode2",
            "industry_code_3":"naeringskode3", "website":"hjemmeside", "email":"epostadresse",
            "phone":"telefon", "mobile":"mobil", "municipality":"kommune", "employees":"antallAnsatte",
            "vat_registered":"registrertIMvaregisteret", "enterprise_register_registered":"registrertIForetaksregisteret",
            "foundation_register_registered":"registrertIStiftelsesregisteret", "voluntary_register_registered":"registrertIFrivillighetsregisteret",
            "bankruptcy":"konkurs", "under_liquidation":"underAvvikling",
            "under_compulsory_liquidation":"underTvangsavviklingEllerTvangsopplosning", "deleted_at":"slettedato",
            "updated_at":"oppdateringsdato"}
    facts={}
    for key,source_key in fields.items():
        value=raw.get(source_key)
        if value is None: continue
        source_date=updated if isinstance(updated,str) else None
        facts[key]=fact(value, source_url, retrieved, source_date, True)
        facts[key]["content_sha256"]=content_sha256
    address=raw.get("forretningsadresse")
    if isinstance(address,dict) and address.get("kommune") and "municipality" not in facts:
        facts["municipality"]=fact(address["kommune"],source_url,retrieved,updated if isinstance(updated,str) else None,True,True)
        facts["municipality"]["content_sha256"]=content_sha256
    return {"org_number":org,"facts":facts,"retrieved_at":retrieved,"source":"Brønnøysundregistrene Enhetsregisteret","warnings":[]}

class Store:
    def __init__(self,path):
        self.path=Path(path); self.path.parent.mkdir(parents=True,exist_ok=True)
        self.db=sqlite3.connect(self.path); self.db.execute("PRAGMA foreign_keys=ON")
        self.db.executescript('''CREATE TABLE IF NOT EXISTS profiles(org_number TEXT PRIMARY KEY, retrieved_at TEXT NOT NULL, last_success TEXT NOT NULL, source TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS facts(org_number TEXT NOT NULL, name TEXT NOT NULL, value_json TEXT NOT NULL, source_url TEXT NOT NULL, retrieved_at TEXT NOT NULL, source_updated_at TEXT, verified INTEGER NOT NULL, derived INTEGER NOT NULL DEFAULT 0, content_sha256 TEXT, PRIMARY KEY(org_number,name), FOREIGN KEY(org_number) REFERENCES profiles(org_number));
        CREATE TABLE IF NOT EXISTS history(id INTEGER PRIMARY KEY, org_number TEXT NOT NULL, name TEXT NOT NULL, value_json TEXT NOT NULL, source_url TEXT NOT NULL, retrieved_at TEXT NOT NULL, source_updated_at TEXT, verified INTEGER NOT NULL, change_type TEXT NOT NULL, old_value_json TEXT, content_sha256 TEXT, old_source_url TEXT, old_retrieved_at TEXT, old_source_updated_at TEXT, old_verified INTEGER, old_derived INTEGER, old_content_sha256 TEXT);
        CREATE TABLE IF NOT EXISTS refresh_runs(id INTEGER PRIMARY KEY, org_number TEXT NOT NULL, retrieved_at TEXT NOT NULL, status TEXT NOT NULL, error TEXT);
        CREATE TABLE IF NOT EXISTS collection_state(key TEXT PRIMARY KEY, value TEXT NOT NULL);'''); self.db.commit()
        for table, column, definition in (("facts","content_sha256","TEXT"),("history","old_value_json","TEXT"),("history","content_sha256","TEXT"),("history","old_source_url","TEXT"),("history","old_retrieved_at","TEXT"),("history","old_source_updated_at","TEXT"),("history","old_verified","INTEGER"),("history","old_derived","INTEGER"),("history","old_content_sha256","TEXT")):
            names={row[1] for row in self.db.execute(f"PRAGMA table_info({table})")}
            if column not in names: self.db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
        self.db.commit()
    def save(self, profile):
        errors=validate_profile(profile)
        if errors: raise ValueError("invalid profile: "+"; ".join(errors))
        org=profile["org_number"]; stamp=profile["retrieved_at"]
        previous=self.db.execute("SELECT retrieved_at FROM profiles WHERE org_number=?",(org,)).fetchone()
        if previous:
            old_time=datetime.fromisoformat(previous[0].replace("Z","+00:00")); new_time=datetime.fromisoformat(stamp.replace("Z","+00:00"))
            if new_time < old_time: return {"added":0,"changed":0,"unchanged":0,"stale":True}
        cur=self.db.execute("SELECT name,value_json,source_url,retrieved_at,source_updated_at,verified,derived,content_sha256 FROM facts WHERE org_number=?",(org,)); old={r[0]:{"value_json":r[1],"source_url":r[2],"retrieved_at":r[3],"source_updated_at":r[4],"verified":r[5],"derived":r[6],"content_sha256":r[7]} for r in cur.fetchall()}
        added=changed=unchanged=0
        self.db.execute("INSERT INTO profiles VALUES(?,?,?,?) ON CONFLICT(org_number) DO UPDATE SET retrieved_at=excluded.retrieved_at,last_success=excluded.last_success,source=excluded.source",(org,stamp,stamp,profile.get("source","Brønnøysundregistrene")))
        for name,item in profile["facts"].items():
            encoded=json.dumps(item["value"],ensure_ascii=False,sort_keys=True)
            if name in old and old[name]["value_json"]==encoded:
                unchanged+=1
                self.db.execute("UPDATE facts SET source_url=?,retrieved_at=?,source_updated_at=?,verified=?,derived=?,content_sha256=? WHERE org_number=? AND name=?",(item["source_url"],item["retrieved_at"],item.get("source_updated_at"),int(item["verified"]),int(item.get("derived",False)),item.get("content_sha256"),org,name))
                continue
            kind="added" if name not in old else "changed"
            prior=old.get(name,{})
            self.db.execute("INSERT INTO history(org_number,name,value_json,source_url,retrieved_at,source_updated_at,verified,change_type,old_value_json,content_sha256,old_source_url,old_retrieved_at,old_source_updated_at,old_verified,old_derived,old_content_sha256) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",(org,name,encoded,item["source_url"],item["retrieved_at"],item.get("source_updated_at"),int(item["verified"]),kind,prior.get("value_json"),item.get("content_sha256"),prior.get("source_url"),prior.get("retrieved_at"),prior.get("source_updated_at"),prior.get("verified"),prior.get("derived"),prior.get("content_sha256")))
            self.db.execute("INSERT INTO facts(org_number,name,value_json,source_url,retrieved_at,source_updated_at,verified,derived,content_sha256) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(org_number,name) DO UPDATE SET value_json=excluded.value_json,source_url=excluded.source_url,retrieved_at=excluded.retrieved_at,source_updated_at=excluded.source_updated_at,verified=excluded.verified,derived=excluded.derived,content_sha256=excluded.content_sha256",(org,name,encoded,item["source_url"],item["retrieved_at"],item.get("source_updated_at"),int(item["verified"]),int(item.get("derived",False)),item.get("content_sha256")))
            if kind=="added": added+=1
            else: changed+=1
        self.db.commit(); return {"added":added,"changed":changed,"unchanged":unchanged}
    def record_failure(self,org,error):
        self.db.execute("INSERT INTO refresh_runs(org_number,retrieved_at,status,error) VALUES(?,?,?,?)",(org,utcnow(),"failed",str(error))); self.db.commit()
    def all(self, limit=None):
        q="SELECT org_number,retrieved_at,source FROM profiles ORDER BY org_number"+(" LIMIT ?" if limit is not None else "")
        for org,stamp,source in self.db.execute(q,((limit,) if limit is not None else ())):
            facts={}
            for name,value,url,ret,upd,ver,der,content_hash in self.db.execute("SELECT name,value_json,source_url,retrieved_at,source_updated_at,verified,derived,content_sha256 FROM facts WHERE org_number=?",(org,)):
                facts[name]={"value":json.loads(value),"source_url":url,"retrieved_at":ret,"source_updated_at":upd,"verified":bool(ver),"derived":bool(der),"content_sha256":content_hash}
            yield {"org_number":org,"retrieved_at":stamp,"source":source,"facts":facts,"warnings":[]}
    def count(self): return self.db.execute("SELECT COUNT(*) FROM profiles").fetchone()[0]
    def collection_cursor(self):
        row=self.db.execute("SELECT value FROM collection_state WHERE key='next_page'").fetchone()
        return int(row[0]) if row else 0
    def set_collection_cursor(self,next_page):
        self.db.execute("INSERT INTO collection_state(key,value) VALUES('next_page',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",(str(next_page),)); self.db.commit()
    def get_profile(self, org):
        row=self.db.execute("SELECT retrieved_at,source FROM profiles WHERE org_number=?",(org,)).fetchone()
        if not row: return None
        facts={}
        for name,value,url,ret,upd,ver,der,content_hash in self.db.execute("SELECT name,value_json,source_url,retrieved_at,source_updated_at,verified,derived,content_sha256 FROM facts WHERE org_number=?",(org,)):
            facts[name]={"value":json.loads(value),"source_url":url,"retrieved_at":ret,"source_updated_at":upd,"verified":bool(ver),"derived":bool(der),"content_sha256":content_hash}
        return {"org_number":org,"retrieved_at":row[0],"source":row[1],"facts":facts}
    def close(self): self.db.close()

def export_jsonl(store,path):
    Path(path).parent.mkdir(parents=True,exist_ok=True)
    with open(path,"w",encoding="utf-8",newline="\n") as f:
        for p in store.all(): f.write(json.dumps(p,ensure_ascii=False,sort_keys=True)+"\n")
def validate_jsonl(path):
    errors=[]; seen=set(); count=0
    with open(path,encoding="utf-8") as f:
        for line_no,line in enumerate(f,1):
            try: p=json.loads(line)
            except json.JSONDecodeError as e: errors.append(f"line {line_no}: {e}"); continue
            count+=1; errs=validate_profile(p)
            if p.get("org_number") in seen: errs.append("duplicate organization number")
            seen.add(p.get("org_number"))
            if errs: errors.extend(f"line {line_no}: {e}" for e in errs)
    return {"records":count,"distinct_org_numbers":len(seen),"valid":not errors,"errors":errors}
