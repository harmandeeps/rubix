start=`date +%s`
/usr/lib/presto/bin/presto client --execute "select * from nation_s3_txt_user_overridden"
end=`date +%s`

runtime=$((end-start))
echo $runtime