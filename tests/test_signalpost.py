import json, tempfile, unittest, csv
from contextlib import redirect_stdout
from io import StringIO
from types import SimpleNamespace
from unittest.mock import patch
from pathlib import Path
from urllib.error import HTTPError
from signalpost.core import BrregClient, BrregRequestError, Store, extract_profile, normalize_org_number, validate_evidence, validate_jsonl

ORG="923456783"
FIXTURE={"organisasjonsnummer":ORG,"navn":"Test Example AS","organisasjonsform":{"kode":"AS"," beskrivelse":"Aksjeselskap"},"registreringsdatoEnhetsregisteret":"2020-01-01","forretningsadresse":{"adresse":["Gate 1"],"postnummer":"0001","poststed":"OSLO","kommune":"Oslo","kommunenummer":"0301","land":"Norge"},"naeringskode1":{"kode":"62.010","beskrivelse":"Programmeringstjenester"},"oppdateringsdato":"2024-01-01"}

class Response:
    def __init__(self,data): self.data=data
    def __enter__(self): return self
    def __exit__(self,*args): pass
    def read(self): return json.dumps(self.data).encode()

class Tests(unittest.TestCase):
    def test_org(self):
        self.assertEqual(normalize_org_number("923 456 783"),ORG)
        with self.assertRaises(ValueError): normalize_org_number("123456789")
        with self.assertRaises(ValueError): normalize_org_number("923456780")
    def test_lookup_fixture(self):
        c=BrregClient(opener=lambda *a,**k:Response(FIXTURE),delay=0)
        profile=c.lookup(ORG); self.assertEqual(profile["facts"]["name"]["value"],"Test Example AS"); self.assertTrue(profile["facts"]["name"]["source_url"].startswith("https://")); self.assertEqual(len(profile["facts"]["name"]["content_sha256"]),64)
    def test_missing_http_and_budget(self):
        def missing(req,timeout): raise HTTPError(req.full_url,404,"not found",{},None)
        self.assertIsNone(BrregClient(opener=missing,delay=0).lookup(ORG))
        c=BrregClient(max_requests=0)
        with self.assertRaises(RuntimeError): c.lookup(ORG)
    def test_identity_mismatch(self):
        bad=dict(FIXTURE,organisasjonsnummer="923456790")
        with self.assertRaises(ValueError): BrregClient(opener=lambda *a,**k:Response(bad),delay=0).lookup(ORG)
    def test_evidence(self):
        p=extract_profile(FIXTURE,"https://data.brreg.no/enhetsregisteret/api/enheter/"+ORG)
        self.assertTrue(all(validate_evidence(x) for x in p["facts"].values()))
        self.assertEqual(p["facts"]["organization_form"]["value"]["kode"],"AS")
        self.assertEqual(p["facts"]["business_address"]["value"]["kommune"],"Oslo")
        self.assertEqual(p["facts"]["industry_code_1"]["value"]["kode"],"62.010")
        bad=dict(next(iter(p["facts"].values())),retrieved_at="2026-01-01T00:00:00")
        self.assertFalse(validate_evidence(bad))
    def test_duplicate_and_history(self):
        with tempfile.TemporaryDirectory() as d:
            s=Store(Path(d)/"x.db"); p=extract_profile(FIXTURE,"https://data.brreg.no/enhetsregisteret/api/enheter/"+ORG)
            self.assertGreater(s.save(p)["added"],0); self.assertEqual(s.save(p)["unchanged"],len(p["facts"]))
            changed=dict(FIXTURE,navn="Changed AS"); s.save(extract_profile(changed,"https://data.brreg.no/enhetsregisteret/api/enheter/"+ORG))
            self.assertGreater(s.db.execute("select count(*) from history where change_type='changed'").fetchone()[0],0); self.assertEqual(s.count(),1); s.close()
    def test_history_before_value_stale_rejection_and_zero_limit(self):
        with tempfile.TemporaryDirectory() as d:
            s=Store(Path(d)/"history.db")
            first=extract_profile(FIXTURE,"https://data.brreg.no/enhetsregisteret/api/enheter/"+ORG)
            first["retrieved_at"]="2026-10-17T16:00:00Z"
            for item in first["facts"].values(): item["retrieved_at"]=first["retrieved_at"]
            s.save(first)
            changed=json.loads(json.dumps(first)); changed["retrieved_at"]="2026-10-17T16:01:00Z"; changed["facts"]["name"]["value"]="Changed AS"
            changed["facts"]["name"]["retrieved_at"]=changed["retrieved_at"]
            s.save(changed)
            old=s.db.execute("select old_value_json,old_source_url,old_retrieved_at from history where name='name' and change_type='changed'").fetchone()
            self.assertEqual(json.loads(old[0]),"Test Example AS"); self.assertEqual(old[1],"https://data.brreg.no/enhetsregisteret/api/enheter/"+ORG); self.assertEqual(old[2],"2026-10-17T16:00:00Z")
            older=json.loads(json.dumps(first)); older["retrieved_at"]="2026-10-17T15:59:00Z"
            self.assertTrue(s.save(older)["stale"])
            self.assertEqual(len(list(s.all(0))),0)
            self.assertEqual(next(s.all())["facts"]["name"]["value"],"Changed AS")
            s.close()
    def test_incomplete_refresh_preserves_verified_facts(self):
        with tempfile.TemporaryDirectory() as d:
            s=Store(Path(d)/"safe.db")
            full=extract_profile(FIXTURE,"https://data.brreg.no/enhetsregisteret/api/enheter/"+ORG); s.save(full)
            partial=extract_profile({"organisasjonsnummer":ORG,"navn":"Test Example AS"},"https://data.brreg.no/enhetsregisteret/api/enheter/"+ORG); s.save(partial)
            current=next(s.all())
            self.assertIn("industry_code_1",current["facts"])
            self.assertEqual(current["facts"]["name"]["value"],"Test Example AS")
            s.close()
    def test_end_to_end_offline_fixture(self):
        with tempfile.TemporaryDirectory() as d:
            path=Path(d)/"db.sqlite3"; out=Path(d)/"profiles.jsonl"
            store=Store(path)
            client=BrregClient(opener=lambda *a,**k:Response(FIXTURE),delay=0)
            profile=client.lookup(ORG); result=store.save(profile)
            self.assertEqual(result["added"],len(profile["facts"]))
            from signalpost.core import export_jsonl
            export_jsonl(store,out); self.assertTrue(validate_jsonl(out)["valid"])
            store.close()
    def test_pagination_and_budget(self):
        calls=[]
        def op(req,timeout):
            calls.append(req.full_url)
            return Response({"_embedded":{"enheter":[FIXTURE]},"page":{"totalPages":1}})
        c=BrregClient(max_requests=1,opener=op,delay=0)
        self.assertEqual(len(list(c.discover(1))),1); self.assertEqual(c.requests,1)
        with self.assertRaises(RuntimeError): c.get("/enheter")
    def test_collection_resume_cursor(self):
        with tempfile.TemporaryDirectory() as d:
            s=Store(Path(d)/"cursor.sqlite3"); self.assertEqual(s.collection_cursor(),0)
            s.set_collection_cursor(7); self.assertEqual(s.collection_cursor(),7); s.close()
        seen=[]
        def op(req,timeout):
            seen.append(req.full_url)
            return Response({"_embedded":{"enheter":[FIXTURE]},"page":{"totalPages":3}})
        c=BrregClient(max_requests=1,opener=op,delay=0)
        list(c.discover(1,start_page=1,on_page=lambda nxt:seen.append(f"next={nxt}")))
        self.assertIn("page=1",seen[0]); self.assertIn("next=2",seen)
    def test_transient_retry_count(self):
        calls=[]
        def op(req,timeout):
            calls.append(req.full_url)
            if len(calls)==1: raise HTTPError(req.full_url,503,"busy",{},None)
            return Response(FIXTURE)
        c=BrregClient(max_requests=2,retries=1,opener=op,delay=0)
        self.assertEqual(c.lookup(ORG)["org_number"],ORG); self.assertEqual(c.requests,2)
    def test_batch_cli_exact_contract_one_output_per_input(self):
        from signalpost.cli import main
        with tempfile.TemporaryDirectory() as d:
            root=Path(d); org2="810034882"
            batch=root/"input.jsonl"; batch.write_text("\n".join(json.dumps({"organisation_number":x}) for x in (ORG,org2,"923456789"))+"\n",encoding="utf-8")
            bulk=root/"registry.csv"
            with bulk.open("w",newline="",encoding="utf-8") as f:
                w=csv.DictWriter(f,fieldnames=["organisasjonsnummer","navn","organisasjonsform.kode","forretningsadresse.kommune"]); w.writeheader(); w.writerow({"organisasjonsnummer":ORG,"navn":"Snapshot AS","organisasjonsform.kode":"AS","forretningsadresse.kommune":"OSLO"})
            out=root/"envelopes.jsonl"; profiles=root/"profiles.jsonl"; report=root/"report.json"; db=root/"db.sqlite3"
            def fake_lookup(client,org):
                client.requests+=1
                if org==org2: raise BrregRequestError("service unavailable",503)
                return extract_profile(dict(FIXTURE,organisasjonsnummer=org),"https://data.brreg.no/enhetsregisteret/api/enheter/"+org,""+org,"a"*64)
            args=["--organisations",str(batch),"--bulk",str(bulk),"--output",str(out),"--profiles-output",str(profiles),"--report",str(report),"--run-id","offline-smoke","--expected-count","3","--db",str(db),"--max-requests","10","--retries","0"]
            with patch("signalpost.batch.BrregClient.lookup",fake_lookup): self.assertEqual(main(args),0)
            rows=[json.loads(x) for x in out.read_text(encoding="utf-8").splitlines()]
            self.assertEqual([x["organisation_number"] for x in rows],[ORG,org2,"923456789"])
            self.assertEqual([x["state"] for x in rows],["available","failed","not_applicable"])
            self.assertEqual(json.loads(report.read_text(encoding="utf-8"))["operations"]["requests"],2)
            self.assertTrue(json.loads(report.read_text(encoding="utf-8"))["validation"]["passed"])
            self.assertTrue(all(x["run"]["run_id"]=="offline-smoke" for x in rows))
    def test_refresh_age_policy_skips_recent_rows(self):
        from signalpost.cli import main
        with tempfile.TemporaryDirectory() as d:
            db=Path(d)/"fresh.sqlite3"; export=Path(d)/"profiles.jsonl"; s=Store(db)
            s.save(extract_profile(FIXTURE,"https://data.brreg.no/enhetsregisteret/api/enheter/"+ORG)); s.close()
            with patch("signalpost.core.BrregClient.lookup",side_effect=AssertionError("fresh row must not be requested")),redirect_stdout(StringIO()) as capture:
                code=main(["--db",str(db),"--jsonl",str(export),"refresh","--limit","1","--max-requests","1","--min-age-hours","24"])
            summary=json.loads(capture.getvalue()); self.assertEqual(code,0); self.assertEqual(summary["requests"],0); self.assertEqual(summary["skipped_fresh"],1)
    def test_export_import_validation(self):
        with tempfile.TemporaryDirectory() as d:
            s=Store(Path(d)/"x.db"); p=extract_profile(FIXTURE,"https://data.brreg.no/enhetsregisteret/api/enheter/"+ORG); s.save(p)
            out=Path(d)/"profiles.jsonl"
            from signalpost.core import export_jsonl
            export_jsonl(s,out); report=validate_jsonl(out); self.assertTrue(report["valid"]); self.assertEqual(report["records"],1); s.close()

if __name__=="__main__": unittest.main()
