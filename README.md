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

Supposing this tool is installed into your PATH, operation is simple.

`git index`

Default operation is to only operate on the current branch.
In the future we will support branches, and name the index after the repo and branch name.

The tool will only index SHAs it has not yet seen, so it's safe to symlink this as a git post recieve hook.

Said hook will read ~/elastest.conf and update ES for the configured branches on the configured remote.

TODO: make this tool independent of App::Prove::Elasticsearch.
