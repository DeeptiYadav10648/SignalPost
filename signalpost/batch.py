"""Compatibility runner for Builderr's one-envelope-per-input batch contract."""
from __future__ import annotations

import csv, gzip, hashlib, json, time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from .core import BrregClient, BrregRequestError, extract_profile, fact, normalize_org_number, utcnow

BULK_URL = "https://data.brreg.no/enhetsregisteret/api/enheter/lastned/csv"
FIELD_NAMES = {
    "name":"navn", "registration_date":"registreringsdatoEnhetsregisteret", "website":"hjemmeside",
    "employees":"antallAnsatte", "vat_registered":"registrertIMvaregisteret", "bankruptcy":"konkurs",
    "under_liquidation":"underAvvikling", "source_updated_at":"oppdateringsdato",
}

def read_inputs(path):
    source=Path(path); text=source.read_text(encoding="utf-8-sig")
    if source.suffix.casefold()==".json":
        payload=json.loads(text); values=payload if isinstance(payload,list) else payload.get("organisation_numbers",[])
    elif source.suffix.casefold()==".jsonl": values=[json.loads(line) for line in text.splitlines() if line.strip()]
    else: values=[line.strip() for line in text.splitlines() if line.strip()]
    result=[]
    for item in values:
        value=item.get("organisation_number",item.get("org_number")) if isinstance(item,dict) else item
        digits="".join(ch for ch in str(value or "") if ch.isdigit())
        result.append({"raw":str(value or ""),"org_number":digits if len(digits)==9 else str(value or ""),"input":item})
    return result

def _hash_file(path):
    h=hashlib.sha256()
    with open(path,"rb") as f:
        for block in iter(lambda:f.read(1024*1024),b""): h.update(block)
    return h.hexdigest()

def _field(row,*names):
    return next((row[n] for n in names if n in row and row[n] not in (None,"")),None)

def _bool(value):
    if value is None: return None
    text=str(value).strip().casefold()
    return True if text=="true" else False if text=="false" else None

def _address(row,prefix):
    mapping={"adresse":"adresse","postnummer":"postnummer","poststed":"poststed","kommune":"kommune","kommunenummer":"kommunenummer","land":"land","landkode":"landkode"}
    value={key:_field(row,prefix+key,"".join([prefix[0].upper(),prefix[1:]])+key) for key,key_src in mapping.items() if _field(row,prefix+key,"".join([prefix[0].upper(),prefix[1:]])+key) is not None}
    # Registry CSV versions may include a list-valued address as one JSON-ish column.
    return value or None

def load_bulk(path,wanted=None):
    if not path: return {},None
    wanted=set(wanted or ())
    if not wanted: return {},None
    checksum=_hash_file(path); opener=gzip.open if str(path).casefold().endswith(".gz") else open
    rows={}
    with opener(path,"rt",encoding="utf-8-sig",newline="") as f:
        for row in csv.DictReader(f):
            raw_org=_field(row,"organisasjonsnummer","Organisasjonsnummer")
            try: org=normalize_org_number(raw_org)
            except (TypeError,ValueError): continue
            if wanted and org not in wanted: continue
            if org in rows: continue
            values={"org_number":org}
            for name,key in FIELD_NAMES.items():
                value=_field(row,key,key[0].upper()+key[1:])
                if name in ("bankruptcy","under_liquidation","vat_registered"): value=_bool(value)
                if name=="employees" and value is not None:
                    try: value=int(value)
                    except (TypeError,ValueError): value=None
                if value is not None: values[name]=value
            form=_field(row,"organisasjonsform.kode","Organisasjonsform.kode")
            form_desc=_field(row,"organisasjonsform.beskrivelse","Organisasjonsform.beskrivelse")
            if form or form_desc: values["organization_form"]={k:v for k,v in (("kode",form),("beskrivelse",form_desc)) if v is not None}
            for key,prefix in (("business_address","forretningsadresse."),("postal_address","postadresse.")):
                address=_address(row,prefix)
                if address: values[key]=address
            for n in (1,2,3):
                code=_field(row,f"naeringskode{n}.kode",f"Næringskode{n}.kode")
                desc=_field(row,f"naeringskode{n}.beskrivelse",f"Næringskode{n}.beskrivelse")
                if code or desc: values[f"industry_code_{n}"]={k:v for k,v in (("kode",code),("beskrivelse",desc)) if v is not None}
            for name,col in (("municipality","forretningsadresse.kommune"),("enterprise_register_registered","registrertIForetaksregisteret"),("foundation_register_registered","registrertIStiftelsesregisteret"),("voluntary_register_registered","registrertIFrivillighetsregisteret")):
                value=_field(row,col,col[0].upper()+col[1:])
                if name.endswith("registered"): value=_bool(value)
                if value is not None: values[name]=value
            values["latest_submitted_accounts"]=_field(row,"sisteInnsendteAarsregnskap","Siste innsendte årsregnskap")
            retrieved=utcnow(); src_updated=values.get("source_updated_at")
            source=BULK_URL
            facts={key:fact(val,source,retrieved,src_updated,True,False) for key,val in values.items() if key!="source_updated_at"}
            for value in facts.values(): value["content_sha256"]=checksum
            rows[org]={"org_number":org,"facts":facts,"retrieved_at":retrieved,"source":"Brønnøysundregistrene frozen bulk registry","bulk_sha256":checksum}
            if wanted and len(rows)>=len(wanted): break
    return rows,checksum

def _ident(org,field,source_url,content_hash):
    return "ev-"+hashlib.sha256(f"{org}|{field}|{source_url}|{content_hash or ''}".encode()).hexdigest()[:20]

def validate_batch_envelopes(envelopes, inputs, expected_count):
    allowed={"available","not_available","blocked","not_applicable","ambiguous","failed"}; errors=[]
    if len(envelopes)!=len(inputs) or len(envelopes)!=expected_count: errors.append("envelope count does not match input/expected count")
    for index,envelope in enumerate(envelopes):
        if index<len(inputs) and envelope.get("organisation_number")!=inputs[index]["org_number"]: errors.append(f"row {index+1}: input order or organisation number mismatch")
        if envelope.get("state") not in allowed: errors.append(f"row {index+1}: invalid terminal state")
        evidence={item.get("id"):item for item in envelope.get("evidence",[]) if item.get("id")}
        for item in evidence.values():
            url=urlparse(str(item.get("source_url") or ""))
            if url.scheme!="https" or not url.netloc: errors.append(f"row {index+1}: invalid evidence URL")
            try:
                dt=datetime.fromisoformat(str(item.get("retrieved_at","")).replace("Z","+00:00"))
                if dt.tzinfo is None or dt.utcoffset().total_seconds()!=0: raise ValueError()
            except (TypeError,ValueError): errors.append(f"row {index+1}: invalid evidence retrieval timestamp")
        for claim in envelope.get("claims",[]):
            refs=claim.get("evidence_ids",[])
            if not refs or any(ref not in evidence for ref in refs): errors.append(f"row {index+1}: claim has missing evidence reference")
    return {"passed":not errors,"checks":{"count":len(envelopes)==len(inputs)==expected_count,"input_order":not any("order" in e for e in errors),"terminal_states":not any("state" in e for e in errors),"evidence_references":not any("evidence reference" in e for e in errors),"evidence_urls_and_timestamps":not any("URL" in e or "timestamp" in e for e in errors)},"errors":errors}

def run_batch(args,store,client_factory=BrregClient,lookup=None):
    started=utcnow(); clock=time.monotonic(); deadline=clock+max(1,args.deadline_seconds)
    inputs=read_inputs(args.organisations); wanted={digits for item in inputs if len(digits:=item["org_number"])==9 and digits.isdigit()}
    bulk,bulk_hash=load_bulk(args.bulk,wanted)
    if len(inputs)!=args.expected_count: raise ValueError(f"expected {args.expected_count} inputs; received {len(inputs)}")
    run_id=args.run_id; envelopes=[]; profiles=[]; states={}
    shared_client=client_factory(max_requests=args.max_requests,timeout=args.timeout,retries=args.retries,delay=args.delay)
    for item in inputs:
        org=item["org_number"]; company_start=time.monotonic(); before=store.get_profile(org) if len(org)==9 and org.isdigit() else None
        evidence_rows={}; candidate_facts={}; errors=[]; changes=[]; status="available"; live_status="failed"; client=None; request_delta=0
        try: normalized=normalize_org_number(org)
        except ValueError as exc:
            normalized=org; status="not_applicable"; errors.append({"source":"input","message":str(exc)})
        if status!="not_applicable":
            base=bulk.get(normalized)
            if base:
                for key,ev in base["facts"].items(): candidate_facts[key]=dict(ev)
            if time.monotonic()>=deadline:
                errors.append({"source":"runtime_budget","message":"run deadline reached before live lookup"}); status="failed"
            else:
                try:
                    client=shared_client; before_requests=client.requests
                    live=lookup(client,normalized) if lookup else client.lookup(normalized)
                    request_delta=client.requests-before_requests
                    if live is None:
                        live_status="not_available"
                        if not base: status="not_available"
                        errors.append({"source":"brreg_entity","message":"registry returned HTTP 404"})
                    else:
                        live_status="available"
                        for key,ev in live["facts"].items():
                            old=candidate_facts.get(key)
                            if old is not None and old["value"]!=ev["value"]:
                                changes.append({"field":key,"before":old["value"],"after":ev["value"],"observed_at":ev["retrieved_at"]})
                            candidate_facts[key]=dict(ev)
                except ValueError as exc:
                    request_delta=client.requests-before_requests if client else 0
                    live_status="ambiguous"; status="ambiguous"; errors.append({"source":"brreg_entity","message":str(exc)})
                except BrregRequestError as exc:
                    request_delta=client.requests-before_requests if client else 0
                    live_status="blocked" if exc.status in (401,403,429) else "failed"
                    status=live_status
                    errors.append({"source":"brreg_entity","message":str(exc),"http_status":exc.status})
                except Exception as exc:
                    request_delta=client.requests-before_requests if client else 0
                    live_status="failed"; status="failed"; errors.append({"source":"brreg_entity","message":f"{type(exc).__name__}: {exc}"})
        if before:
            for key,old_fact in before["facts"].items(): candidate_facts.setdefault(key,dict(old_fact))
            prior_changes={item["field"]:item for item in changes}
            for key,new_fact in candidate_facts.items():
                old_fact=before["facts"].get(key)
                if old_fact and json.dumps(old_fact["value"],sort_keys=True,ensure_ascii=False)!=json.dumps(new_fact["value"],sort_keys=True,ensure_ascii=False):
                    prior_changes[key]={"field":key,"before":old_fact["value"],"after":new_fact["value"],"observed_at":new_fact["retrieved_at"]}
            changes=list(prior_changes.values())
        # A frozen official registry match remains useful evidence when live lookup is unavailable.
        if status in ("failed","blocked","not_available") and candidate_facts and bulk.get(normalized): status="available"
        elif status in ("failed","blocked") and candidate_facts and before: status="available"
        if not candidate_facts and status=="available": status="not_available"
        evidence_ids={}; facts={}
        for key,ev in candidate_facts.items():
            source_url=ev["source_url"]; content_hash=ev.get("content_sha256")
            eid=_ident(normalized,key,source_url,content_hash); evidence_ids[key]=eid
            evidence_rows[eid]={"id":eid,"source_url":source_url,"source_class":"official_registry","retrieved_at":ev["retrieved_at"],"source_updated_at":ev.get("source_updated_at"),"content_sha256":content_hash,"claim_span":json.dumps({key:ev["value"]},ensure_ascii=False,sort_keys=True,separators=(",",":")),"verified":bool(ev.get("verified")),"derived":bool(ev.get("derived"))}
            if source_url==BULK_URL: evidence_rows[eid]["source_row_key"]=normalized
            facts[key]={"value":ev["value"],"source_url":source_url,"retrieved_at":ev["retrieved_at"],"source_updated_at":ev.get("source_updated_at"),"verified":bool(ev.get("verified")),"derived":bool(ev.get("derived")),"evidence_id":eid}
        claims=[{"field":key,"value":ev["value"],"availability":"available","confidence":1.0 if ev["verified"] else 0.0,"evidence_ids":[evidence_ids[key]]} for key,ev in facts.items()]
        identity={k:facts[k]["value"] for k in ("org_number","name","organization_form","business_address","municipality","industry_code_1") if k in facts}
        # Refresh history is based on semantic values only. A failed lookup never overwrites stored facts.
        if status=="available" and client and live_status=="available":
            source_profile={"org_number":normalized,"facts":candidate_facts,"retrieved_at":max(ev["retrieved_at"] for ev in candidate_facts.values()),"source":"Brønnøysundregistrene Enhetsregisteret"}
            try: store.save(source_profile)
            except (ValueError,KeyError) as exc: errors.append({"source":"storage","message":str(exc)})
        if status in ("failed","blocked","not_available") and claims and bulk.get(normalized): status="available"
        completed=utcnow(); op_requests=locals().get("request_delta",0)
        envelope={"run_id":run_id,"organisation_number":normalized,"state":status,"run":{"run_id":run_id,"started_at":started,"completed_at":completed,"terminal_status":"completed"},"identity":identity,"claims":claims,"evidence":list(evidence_rows.values()),"modules":{"registry":{"state":"available" if bulk.get(normalized) else "not_available","final_timestamp":bulk.get(normalized,{}).get("retrieved_at")},"registry_live":{"state":live_status,"final_timestamp":completed}},"changes":changes,"errors":errors,"operations":{"requests":op_requests,"runtime_ms":round((time.monotonic()-company_start)*1000),"third_party_cost_usd":0}}
        for annotation in ("evaluation_split","sample_slice"):
            if isinstance(item["input"],dict) and item["input"].get(annotation) is not None:
                envelope[annotation]=item["input"][annotation]
        envelopes.append(envelope); states[status]=states.get(status,0)+1
        profile_output={"organisation_number":normalized,"identity":identity,"facts":facts,"evidence":list(evidence_rows.values()),"state":status,"changes":changes,"errors":errors}
        for annotation in ("evaluation_split","sample_slice"):
            if isinstance(item["input"],dict) and item["input"].get(annotation) is not None: profile_output[annotation]=item["input"][annotation]
        profiles.append(profile_output)
    completed=utcnow(); elapsed=round((time.monotonic()-clock)*1000)
    validation=validate_batch_envelopes(envelopes,inputs,args.expected_count)
    report={"run_id":run_id,"started_at":started,"completed_at":completed,"expected_count":args.expected_count,"emitted_envelopes":len(envelopes),"distinct_organisation_numbers":len({x["organisation_number"] for x in envelopes}),"states":states,"operations":{"requests":shared_client.requests,"runtime_ms":elapsed,"third_party_cost_usd":0,"deadline_seconds":args.deadline_seconds,"request_budget":args.max_requests},"validation":validation}
    return envelopes,profiles,report

def write_batch_outputs(args,envelopes,profiles,report):
    def write(path,rows):
        output=Path(path); output.parent.mkdir(parents=True,exist_ok=True); temp=output.with_suffix(output.suffix+".tmp")
        with temp.open("w",encoding="utf-8",newline="\n") as f:
            for row in rows: f.write(json.dumps(row,ensure_ascii=False,sort_keys=True,separators=(",",":"))+"\n")
        temp.replace(output)
    write(args.output,envelopes); write(args.profiles_output,profiles)
    report_path=Path(args.report); report_path.parent.mkdir(parents=True,exist_ok=True); report_path.write_text(json.dumps(report,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
