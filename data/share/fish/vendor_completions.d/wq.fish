complete -c wq -e
complete -c wq -f
# Commands
complete -c wq -n "__fish_use_subcommand" -a "ls" -d "List jobs"
complete -c wq -n "__fish_use_subcommand" -a "lsw" -d "List workers"
complete -c wq -n "__fish_use_subcommand" -a "submit" -d "Submit a job"
complete -c wq -n "__fish_use_subcommand" -a "serve" -d "Start a server"
complete -c wq -n "__fish_use_subcommand" -a "work" -d "Start a worker"
# General options
complete -c wq -s h -l help -d "Show help"
complete -c wq -s F -l config -r -d "Config file"
# Subcommand options
complete -c wq -n "__fish_seen_subcommand_from submit" -F
complete -c wq -n "__fish_seen_subcommand_from submit" -s C -l directory -x -a "(__fish_complete_directories (commandline -ct))" -d "Change directory"
complete -c wq -n "__fish_seen_subcommand_from submit" -s n -l name -x -d "Job name"
complete -c wq -n "__fish_seen_subcommand_from submit" -s R -l resources -x -d "Job resources"
complete -c wq -n "__fish_seen_subcommand_from submit" -s p -l priority -x -d "Job priority"
