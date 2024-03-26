set -e

echo "aws ecs list-tasks --region $1 --cluster $2"
tasks=$(aws ecs list-tasks --region $1 --cluster $2)
echo $tasks
task_arns=$(echo $tasks | jq '.taskArns')

arns_count=$(echo $task_arns | jq '. | length')
if [[ $arns_count == '0' ]]; then
  echo "Cluster doesn't have any tasks" && exit 1
fi
if [[ $arns_count != '1' ]]; then
  echo "Cluster have more than one task" && exit 1
fi

task_id=$(echo $task_arns | jq '.[0]' | awk -F/ '{print $NF}' | tr -d '"')
# for some fucking reason debian doesn't have `pwait` as part of `procps` package,
# so we do the dirty `tail` hack in order to wait before process exits
command="bash -c 'pkill -SIGUSR1 pid1 && while sleep 60; do echo 'Decommissioning...'; done & tail -f --pid=\$(pgrep irn | tr -d '\n') /dev/null'"
echo "aws ecs execute-command --region $1 --cluster $2 --task $task_id --interactive"
echo "command: $command"
aws ecs execute-command --region $1 --cluster $2 --container $2 --task $task_id --command "$command" --interactive
