for i in `cat slaves`
do
  echo 'Deleting data from '$i
  ssh $i "rm -r /s/$i/a/nobackup/cs455/josiahm/dfs/data"
done
