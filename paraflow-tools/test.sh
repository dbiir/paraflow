#! /bin/bash
func ()
{
cat >func.exp<<EOF
#!/usr/bin/expect
spawn ssh presto@192.168.124.15
expect {
"*yes/no*" {send "yes\r"; exp_continue}
"*password*" {send "presto455\r";}
}
expect "*#"
send "cd /home/presto\r"
expect "*#"
send "mkdir paraflow\r"
expect "*#"
send "exit\r"
expect eof
EOF
chmod 755 func.exp
./func.exp > /dev/null
}
func
echo "succeed"
echo "end"
