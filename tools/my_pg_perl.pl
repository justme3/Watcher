#!/usr/bin/perl
use strict;
use DBI;

my $epochtime = 1;
my $cid = 9999999;
my $sid= 99999999;
my $cname ="Mark Sloan J. Dell'olio Associates, LLC";
my $ms_status = 0;
my $ms_user = "journalmailbox";
my $ms_host = "mail.whocares.com";
my $ms_port = 110;
my $ms_type = 2;
my $ms_name = "Exchange 2003 - Envelope Journal";
my $ms_desc = "Microsoft Exchange Server";
my $ms_backlog = 0;




my $pg_dbh = DBI->connect("dbi:Pg:dbname=watcher;host=localhost","postgres","") || die $DBI::errstr;
my $sql =  "INSERT INTO arc_mailsource (ms_epochtime,cid,sid,cstnam,ms_status,ms_host,ms_user,ms_port,ms_type,ms_name,ms_desc,ms_backlog) VALUES (to_timestamp(?),?,?,?,?,?,?,?,?,?,?,?)";
my $sth = $pg_dbh->prepare($sql);
$sth->execute($epochtime, $cid, $sid, $cname, $ms_status, $ms_host, $ms_user, $ms_port, $ms_type, $ms_name, $ms_desc, $ms_backlog);

$sth->finish();
$pg_dbh->disconnect();

 
