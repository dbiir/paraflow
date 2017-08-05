#! /bin/bash
func ()
{
cat >func.exp<<EOF
#!/usr/bin/expect
spawn scp a.sh presto@192.168.124.15:/home/presto/paraflow
expect {
"*yes/no*" {send "yes\r"; exp_continue}
"*password*" {send "presto455\r";}
}
expect eof
EOF
chmod 755 func.exp
./func.exp > /dev/null
}
func
echo "succeed"
echo "end"
