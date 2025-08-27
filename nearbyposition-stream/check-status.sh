curl -s -f http://jobmanager:8081/jobs |jq -r '.jobs[].status' | head -n 1 | xargs test RUNNING = ;
