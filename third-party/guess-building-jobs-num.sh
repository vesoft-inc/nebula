available_mem_mb=$(awk '/^MemFree|^Buffers|^Cached/{s+=$2;}END{print int(s/1024);}' /proc/meminfo 2>/dev/null)
physical_cores=$(lscpu | awk 'BEGIN{s=1;}/Core\(s\) per socket|Socket\(s\)/{s*=$NF;}END{print s;}' 2>/dev/null)
building_jobs_num=0

[[ -n $available_mem_mb ]] && jobs_by_mem=$((available_mem_mb / 1024 / 2))
[[ -n $physical_cores ]] && jobs_by_cpu=$physical_cores

[[ -n $jobs_by_mem ]] && building_jobs_num=$jobs_by_mem
[[ -n $jobs_by_cpu ]] && [[ $jobs_by_cpu -lt $jobs_by_mem ]] && building_jobs_num=$jobs_by_cpu
[[ $building_jobs_num -eq 0 ]] && building_jobs_num=1
