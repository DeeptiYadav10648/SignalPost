from __future__ import annotations
import argparse, json, logging, sqlite3, sys
from pathlib import Path
from .core import BrregClient, Store, export_jsonl, validate_jsonl, utcnow

def main(argv=None):
    argv=list(sys.argv[1:] if argv is None else argv)
    if "--organisations" in argv:
        return batch_main(argv)
    p=argparse.ArgumentParser(prog="signalpost",description="Evidence-backed profiles from the Norwegian Enhetsregisteret")
    p.epilog="Competition runner: python -m signalpost --organisations batch.jsonl --bulk registry.csv --output envelopes.jsonl --profiles-output profiles.jsonl --report run.json --run-id RUN --expected-count N"
    p.add_argument("--db",default="output/signalpost.sqlite3"); p.add_argument("--jsonl",default="output/profiles.jsonl")
    sub=p.add_subparsers(dest="command",required=True)
    q=sub.add_parser("lookup"); q.add_argument("--org-number",required=True); q.add_argument("--max-requests",type=int,default=10)
    q=sub.add_parser("collect"); q.add_argument("--target",type=int,default=1000); q.add_argument("--max-requests",type=int,default=1800); q.add_argument("--page-size",type=int,default=100); q.add_argument("--dry-run",action="store_true")
    q=sub.add_parser("refresh"); q.add_argument("--limit",type=int,default=100); q.add_argument("--max-requests",type=int,default=200); q.add_argument("--min-age-hours",type=float,default=24); q.add_argument("--force",action="store_true",help="Ignore the minimum age and revalidate the selected profiles")
    sub.add_parser("validate"); q=sub.add_parser("export"); q.add_argument("--format",choices=["jsonl"],default="jsonl"); q.add_argument("--output")
    sub.add_parser("stats")
    a=p.parse_args(argv); store=Store(a.db)
    try:
        if a.command=="lookup":
            c=BrregClient(a.max_requests); profile=c.lookup(a.org_number)
            if profile is None: print(json.dumps({"error":"company not found","org_number":a.org_number})); return 2
            print(json.dumps(profile,ensure_ascii=False,indent=2)); print(json.dumps({"stored_changes":store.save(profile),"requests":c.requests}),file=sys.stderr); return 0
        if a.command=="collect":
            if a.dry_run:
                print(json.dumps({"dry_run":True,"target":a.target,"request_budget":a.max_requests,"stored":store.count()})); return 0
            c=BrregClient(a.max_requests); saved=errors=0; failure=None
            try:
                # Resume only at committed page boundaries. An interrupted page is replayed and deduplicated.
                if store.count() < a.target:
                    start_page=min(store.collection_cursor(),99)
                    for profile in c.discover(10**12,start_page=start_page,page_size=a.page_size,on_page=store.set_collection_cursor):
                        try: store.save(profile); saved+=1
                        except (ValueError,sqlite3.Error) as e: errors+=1; logging.warning("Rejected %s: %s",profile.get("org_number"),e)
                        if store.count() >= a.target: break
            except Exception as e: failure=e; logging.error("Collection stopped: %s",e)
            export_jsonl(store,a.jsonl)
            summary={"new_profiles_or_refreshes":saved,"already_stored":max(0,store.count()-saved),"stored_profiles":store.count(),"target_reached":store.count()>=a.target,"resumed_from_page":start_page if store.count()<a.target else store.collection_cursor(),"rejected":errors,"requests":c.requests,"request_budget":a.max_requests}
            Path("output/manifest.json").write_text(json.dumps({"generated_at":utcnow(),"record_count":store.count(),"method":"Official paginated Enhetsregisteret API","source":"https://data.brreg.no/enhetsregisteret/api","validation":validate_jsonl(a.jsonl),"collection":summary},indent=2),encoding="utf-8")
            print(json.dumps(summary)); return 0 if store.count() >= a.target and not failure and not errors else 1
        if a.command=="refresh":
            profiles=list(store.all(a.limit)); c=BrregClient(a.max_requests); successes=failures=not_found=skipped_fresh=0; changes={"added":0,"changed":0,"unchanged":0}
            for old in profiles:
                try:
                    from datetime import datetime,timezone
                    age=(datetime.now(timezone.utc)-datetime.fromisoformat(old["retrieved_at"].replace("Z","+00:00"))).total_seconds()/3600
                    if not a.force and age<a.min_age_hours: skipped_fresh+=1; continue
                    new=c.lookup(old["org_number"])
                    if new:
                        result=store.save(new); successes+=1
                        for k,v in result.items(): changes[k]+=v
                    else:
                        not_found+=1; store.record_failure(old["org_number"],"registry returned HTTP 404")
                except Exception as e: store.record_failure(old["org_number"],e); failures+=1
            export_jsonl(store,a.jsonl); print(json.dumps({"considered":len(profiles),"refreshed":successes,"not_found":not_found,"skipped_fresh":skipped_fresh,"failures":failures,"changes":changes,"requests":c.requests,"request_budget":a.max_requests,"force":a.force})); return 1 if failures else 0
        if a.command=="validate":
            result=validate_jsonl(a.jsonl) if Path(a.jsonl).exists() else {"records":0,"distinct_org_numbers":0,"valid":store.count()==0,"errors":[] if store.count()==0 else ["JSONL export missing"]}
            print(json.dumps(result,indent=2)); return 0 if result["valid"] else 1
        if a.command=="export":
            dest=a.output or a.jsonl; export_jsonl(store,dest); print(json.dumps({"path":dest,"records":store.count()})); return 0
        if a.command=="stats": print(json.dumps({"profiles":store.count(),"database":a.db,"jsonl":a.jsonl})); return 0
    finally: store.close()
    return 2

def batch_main(argv):
    from .batch import run_batch,write_batch_outputs
    p=argparse.ArgumentParser(prog="signalpost",description="Emit one evidence-backed terminal envelope per Builderr input company")
    p.add_argument("--organisations",required=True,help="JSON, JSONL, or one organisation number per line")
    p.add_argument("--bulk",help="Evaluator-supplied Brønnøysund registry CSV snapshot")
    p.add_argument("--output",required=True,help="Terminal envelope JSONL")
    p.add_argument("--profiles-output",required=True); p.add_argument("--report",required=True)
    p.add_argument("--run-id",required=True); p.add_argument("--expected-count",type=int,default=100)
    p.add_argument("--workers",type=int,default=1,help="Accepted for starter compatibility; this build uses bounded sequential requests")
    p.add_argument("--checkpoint-every",type=int,default=25)
    p.add_argument("--modules",default="registry,registry_live")
    p.add_argument("--db",default="output/signalpost.sqlite3")
    p.add_argument("--max-requests",type=int,default=1800); p.add_argument("--deadline-seconds",type=int,default=2400)
    p.add_argument("--timeout",type=float,default=8); p.add_argument("--retries",type=int,default=1); p.add_argument("--delay",type=float,default=.15)
    a=p.parse_args(argv)
    if a.max_requests<0 or a.deadline_seconds<1 or a.timeout<=0 or a.retries<0 or a.delay<0 or a.checkpoint_every<1:
        p.error("budgets, timeout, retries and checkpoint interval must be non-negative (deadline and checkpoint interval positive)")
    unsupported=set(x.strip() for x in a.modules.split(",") if x.strip())-{"registry","registry_live"}
    if unsupported: p.error("this runner supports only registry and registry_live modules; unsupported: "+", ".join(sorted(unsupported)))
    store=Store(a.db)
    try:
        envelopes,profiles,report=run_batch(a,store)
        report["expected_count_match"]=len(envelopes)==a.expected_count
        report["modules_requested"]=[x.strip() for x in a.modules.split(",") if x.strip()]
        report["effective_workers"]=1
        write_batch_outputs(a,envelopes,profiles,report)
        print(json.dumps(report,ensure_ascii=False,indent=2))
        return 0 if len(envelopes)==report["expected_count"] and report["validation"]["passed"] else 1
    except Exception as exc:
        logging.exception("Batch run failed: %s",exc)
        return 1
    finally:
        store.close()

if __name__=="__main__": raise SystemExit(main())
