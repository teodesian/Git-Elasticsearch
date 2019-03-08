# Git-Elasticsearch
Index your git log for branch(es) in elasticsearch

Sort of an add-on to App::Prove::Elasticsearch so you can cross-reference test behavior with code changes.

Stores various fields, broken down *per file*:

commit: relevant SHA
branch: relevant branch
time: relevant time commit got recorded in said branch
message: commit message
diff: diff of changes for a particular file
diffstat: total LoC change (add abs of file add/sub)
add: No. Additions
rem: No. Removals
file: file changed

Means of operation is simple, enter your repository and run es-index-repository.

Default operation is to only operate on the current branch, but a branch may be provided with (--branch), which may be passed multiple times.
Remote-only branches are also accepted, along with arbitrary SHAs corresponding to detached HEADs.

The tool will only index SHAs it has not yet seen, so it's safe to use the provided git post recieve hook.

Said hook will read ~/elastigit.conf and update ES for the configured branches on the configured remote.
