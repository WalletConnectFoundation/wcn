set -e

if [[ $3 == 'true' ]]; then
  echo "Safety switch is ON, preventing node decommissioning" && exit 1
fi

echo "Safety switch is OFF, proceeding to node decommissioning"

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
command="bash -c 'pkill -SIGUSR1 irn'"
echo "aws ecs execute-command --region $1 --cluster $2 --task $task_id --interactive"
echo "command: $command"
aws ecs execute-command --region $1 --cluster $2 --container $2 --task $task_id --command "$command" --interactive

# After receiving SIGUSR1 the node will ignore SIGTERMs sent by the ECS. Make sure that we won't issue SIGTERM too quicky.
# Probably unnecessary, but just in case.
sleep 1
