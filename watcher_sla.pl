#!/usr/bin/perl -w

use strict;
use warnings;
use diagnostics;
use Switch;

use lib '/usr/local/bin/opsadmin/perl/';

use DBI;
use Data::Dumper;
use Getopt::Long qw(:config no_ignore_case bundling);
use XML::Simple;
use XML::XPath;

require "ctime.pl";

our $AUTHOR     = 'John_Vossler@McAfee.com';
our $VERSION    = 0.9;

our %flags = ();

$ENV{'PGPASSWORD'} = 'dbG0d';    # Export the postgres password

sub get_opts();
sub usage();

&get_opts();

if($flags{'debug'})
{
   print "\nflags\n";
   print Dumper(\%flags);
}

##
## Constants
##
my @tspans = (30,60,90,180);

##
## variables
##
my @record = ();
my @records = ();
my $line = ();
my $seg = ();
my @segs = ();
my @ssegs = ();
my @lsegs = ();
my @list = ();
my @solrips1 = ();
my @solrips2 = ();
my $site = ();
my @sites = ();
my @classes = ();
my %seen = ();
my $ip = ();
my $table = ();
my $dest = ();
my $destsrv = ();
my $index = ();
my $nindex = ();
my $timestamp = ();
my $tspan = ();
my $sth = ();
my $dbh = ();
my $psqlcmd = ();
my @locs = ();
my $loc1 = ();
my $loc2 = ();


##
## Get config information for which sites to draw pretty pictures for from table names in DB
##
#
# Get each type of class of SLA data
#
	@classes = `/usr/bin/psql -At -U postgres watcher -c "select relname from pg_class where relname ~ '_sla_'" | grep -v pkey | grep -v lastrun |awk -F\_ '{print \$3}'`;
	chomp(@sites);

#
# Make the list of sites unique
#
	undef %seen;
   @classes = grep(!$seen{$_}++, @classes);
	chomp(@classes);

	if ( $flags{'verbose'} )
	{
		print "\nlist of classes\n";
		print Dumper \@classes;
	}

#
# Get each domain for each class of SLA data
#
	@sites = `/usr/bin/psql -At -U postgres watcher -c "select relname from pg_class where relname ~ '_sla_'" | awk -F\_ '{print \$1}'`;
	chomp(@sites);

#
# Make the list of sites unique
#
	undef %seen;
   @sites = grep(!$seen{$_}++, @sites);
	chomp(@sites);

	if ( $flags{'verbose'} )
	{
		print "\nlist of sites\n";
		print Dumper \@sites;
	}

##
## Now generate the graphics for the web site
##

##
## Create all the functions we will need for graph creation
##
#
# define graphs for the SLA plots for a given US site
#
$dbh = DBI->connect("dbi:Pg:dbname=watcher;host=localhost","postgres","") || die $DBI::errstr;

if ( $flags{'verbose'} )
{
	print "\nconnect status in dbh\n";
	print Dumper $dbh;
}

$psqlcmd = "create or replace function sla_plot_us(varchar, integer) returns void as \$\$

filename<-paste(\"\",arg1,\"-sla-\",arg2,\".png\",sep=\"\");
ffilename<-paste(\"\",\"var\",\"www\",\"html\",\"watcher\",\"sla\",filename,sep=\"/\");
png(ffilename,width = 1200,height = 1600,units = \"px\",pointsize = 16);
par(mfrow=c(5,2),oma=c(0,0,3,0),cex.main=2);

# 1,1
query<<-paste(\"select date_trunc('day',epochtime)::date as sla_date,(case when polls=0 then 0 else ((successes*100.00)/polls) end ) as uptime from \",arg1,\"_sla_tesla where epochtime between (now() - '\",arg2,\" days'::interval) and (now() - '1 day'::interval) and dnslabel ~ 'inbound' \",sep=\"\");
segplot<<-pg.spi.exec(query);
segplot\$sla_date<-as.Date(segplot\$sla_date);
plot(segplot,col=\"red\",pch=20,type=\"p\",usr=c(0,105),xlab=\"date\",ylab=\"% uptime\",main=\"Inbound mail uptime\");
lines(segplot,col=\"blue\",type=\"l\");

# 1,2
query<-paste(\"select date_trunc('day',epochtime)::date as sla_date,(latency*1000.00) as msec from \",arg1,\"_sla_tesla where epochtime between (now() - '\",arg2,\" days'::interval) and (now() - '1 day'::interval) and dnslabel ~ 'inbound' \",sep=\"\");
segplot<-pg.spi.exec(query);
segplot\$sla_date<-as.Date(segplot\$sla_date);
plot(segplot,col=\"red\",pch=20,type=\"p\",xlab=\"date\",ylab=\"Latency ms\",main=\"Inbound Latency\");
lines(segplot,col=\"blue\",type=\"l\");

# 2,1
query<-paste(\"select date_trunc('day',epochtime)::date as sla_date,(case when polls=0 then 0 else ((successes*100.00)/polls) end ) as uptime from \",arg1,\"_sla_tesla where epochtime between (now() - '\",arg2,\" days'::interval) and (now() - '1 day'::interval) and dnslabel ~ 'outbound' \",sep=\"\");
segplot<-pg.spi.exec(query);
segplot\$sla_date<-as.Date(segplot\$sla_date);
plot(segplot,col=\"red\",pch=20,type=\"p\",usr=c(0,105),xlab=\"date\",ylab=\"% uptime\",main=\"Outbound mail uptime\");
lines(segplot,col=\"blue\",type=\"l\");

# 2,2
query<-paste(\"select date_trunc('day',epochtime)::date as sla_date,(latency*1000.00) as msec from \",arg1,\"_sla_tesla where epochtime between (now() - '\",arg2,\" days'::interval) and (now() - '1 day'::interval) and dnslabel ~ 'outbound' \",sep=\"\");
segplot<-pg.spi.exec(query);
segplot\$sla_date<-as.Date(segplot\$sla_date);
plot(segplot,col=\"red\",pch=20,type=\"p\",xlab=\"date\",ylab=\"Latency ms\",main=\"Outbound mail Latency\");
lines(segplot,col=\"blue\",type=\"l\");

# 3,1
query<-paste(\"select date_trunc('day',epochtime)::date as sla_date,(case when polls=0 then 0 else ((successes*100.00)/polls) end ) as uptime from \",arg1,\"_sla_tesla where epochtime between (now() - '\",arg2,\" days'::interval) and (now() - '1 day'::interval) and dnslabel ~ 'web' \",sep=\"\");
segplot<-pg.spi.exec(query);
segplot\$sla_date<-as.Date(segplot\$sla_date);
plot(segplot,col=\"red\",pch=20,type=\"p\",usr=c(0,105),xlab=\"date\",ylab=\"% uptime\",main=\"Web uptime\");
lines(segplot,col=\"blue\",type=\"l\");

# 3,2
query<-paste(\"select date_trunc('day',epochtime)::date as sla_date,(latency*1000.00) as msec from \",arg1,\"_sla_tesla where epochtime between (now() - '\",arg2,\" days'::interval) and (now() - '1 day'::interval) and dnslabel ~ 'web' \",sep=\"\");
segplot<-pg.spi.exec(query);
segplot\$sla_date<-as.Date(segplot\$sla_date);
plot(segplot,col=\"red\",pch=20,type=\"p\",xlab=\"date\",ylab=\"Latency ms\",main=\"Web Latency\");
lines(segplot,col=\"blue\",type=\"l\");

# 4,1
query<-paste(\"select date_trunc('day',epochtime)::date as sla_date,(case when polls=0 then 0 else ((successes*100.00)/polls) end ) as uptime from \",arg1,\"_sla_tesla where epochtime between (now() - '\",arg2,\" days'::interval) and (now() - '1 day'::interval) and ( dnslabel ~ 'portal' or dnslabel ~ 'console' ) \",sep=\"\");
segplot<-pg.spi.exec(query);
segplot\$sla_date<-as.Date(segplot\$sla_date);
plot(segplot,col=\"red\",pch=20,type=\"p\",usr=c(0,105),xlab=\"date\",ylab=\"% uptime\",main=\"Customer Interface Uptime\");
lines(segplot,col=\"blue\",type=\"l\");

# 4,2
query<-paste(\"select date_trunc('day',epochtime)::date as sla_date,(latency*1000.00) as msec from \",arg1,\"_sla_tesla where epochtime between (now() - '\",arg2,\" days'::interval) and (now() - '1 day'::interval) and ( dnslabel ~ 'portal' or dnslabel ~ 'console' ) \",sep=\"\");
segplot<-pg.spi.exec(query);
segplot\$sla_date<-as.Date(segplot\$sla_date);
plot(segplot,col=\"red\",pch=20,type=\"p\",xlab=\"date\",ylab=\"Latency ms\",main=\"Customer Interface Latency\");
lines(segplot,col=\"blue\",type=\"l\");

# 5,2
### arc uptime

# 5,2
### arc latency

title(paste(\"SLA data for \",arg1,\" \",sep=\"\"),outer=TRUE);

dev.off();

system(paste(\"chmod 644 \",ffilename,sep=\"\"));

par(mfrow=c(1,1));

\$\$ language plr;
" ;

$sth = $dbh->prepare($psqlcmd);

if ( $flags{'verbose'} )
{
	print "\nsth - after prepare\n";
	print Dumper $sth;
}

$sth->execute() || die $DBI::errstr;

if ( $flags{'verbose'} )
{
	print "\nsth - after execute\n";
	print Dumper $sth;
}

$sth->finish();
$dbh->disconnect();

#
# define graphs for the SLA plots for a given NON-US site
#
$dbh = DBI->connect("dbi:Pg:dbname=watcher;host=localhost","postgres","") || die $DBI::errstr;

if ( $flags{'verbose'} )
{
	print "\nconnect status in dbh\n";
	print Dumper $dbh;
}

$psqlcmd = "create or replace function sla_plot(varchar, integer) returns void as \$\$

filename<-paste(\"\",arg1,\"-sla-\",arg2,\".png\",sep=\"\");
ffilename<-paste(\"\",\"var\",\"www\",\"html\",\"watcher\",\"sla\",filename,sep=\"/\");
png(ffilename,width = 1200,height = 800,units = \"px\",pointsize = 16);
par(mfrow=c(2,2),oma=c(0,0,3,0),cex.main=2);

###pg.thrownotice(arg1);
###pg.thrownotice(arg2);
# 1,1
query<<-paste(\"select date_trunc('day',epochtime)::date as sla_date,(case when polls=0 then 0 else ((successes*100.00)/polls) end ) as uptime from \",arg1,\"_sla_tesla where epochtime between (now() - '\",arg2,\" days'::interval) and (now() - '1 day'::interval) and dnslabel ~ 'inbound' \",sep=\"\");
segplot<<-pg.spi.exec(query);
segplot\$sla_date<-as.Date(segplot\$sla_date);
plot(segplot,col=\"red\",pch=20,type=\"p\",usr=c(0,105),xlab=\"date\",ylab=\"% uptime\",main=\"Inbound mail uptime\");
lines(segplot,col=\"blue\",type=\"l\");

# 1,2
query<-paste(\"select date_trunc('day',epochtime)::date as sla_date,(latency*1000.00) as msec from \",arg1,\"_sla_tesla where epochtime between (now() - '\",arg2,\" days'::interval) and (now() - '1 day'::interval) and dnslabel ~ 'inbound' \",sep=\"\");
segplot<-pg.spi.exec(query);
segplot\$sla_date<-as.Date(segplot\$sla_date);
plot(segplot,col=\"red\",pch=20,type=\"p\",xlab=\"date\",ylab=\"Latency ms\",main=\"Inbound Latency\");
lines(segplot,col=\"blue\",type=\"l\");

# 2,1
######query<-paste(\"select date_trunc('day',epochtime)::date as sla_date,(case when polls=0 then 0 else ((successes*100.00)/polls) end ) as uptime from \",arg1,\"_sla_tesla where epochtime between (now() - '\",arg2,\" days'::interval) and (now() - '1 day'::interval) and dnslabel ~ 'outbound' \",sep=\"\");
######segplot<-pg.spi.exec(query);
######segplot\$sla_date<-as.Date(segplot\$sla_date);
######plot(segplot,col=\"red\",pch=20,type=\"p\",usr=c(0,105),xlab=\"date\",ylab=\"% uptime\",main=\"Outbound mail uptime\");
######lines(segplot,col=\"blue\",type=\"l\");

# 2,2
######query<-paste(\"select date_trunc('day',epochtime)::date as sla_date,(latency*1000.00) as msec from \",arg1,\"_sla_tesla where epochtime between (now() - '\",arg2,\" days'::interval) and (now() - '1 day'::interval) and dnslabel ~ 'outbound' \",sep=\"\");
######segplot<-pg.spi.exec(query);
######segplot\$sla_date<-as.Date(segplot\$sla_date);
######plot(segplot,col=\"red\",pch=20,type=\"p\",xlab=\"date\",ylab=\"Latency ms\",main=\"Outbound mail Latency\");
######lines(segplot,col=\"blue\",type=\"l\");

# 3,1
query<-paste(\"select date_trunc('day',epochtime)::date as sla_date,(case when polls=0 then 0 else ((successes*100.00)/polls) end ) as uptime from \",arg1,\"_sla_tesla where epochtime between (now() - '\",arg2,\" days'::interval) and (now() - '1 day'::interval) and dnslabel ~ 'web' \",sep=\"\");
segplot<-pg.spi.exec(query);
segplot\$sla_date<-as.Date(segplot\$sla_date);
plot(segplot,col=\"red\",pch=20,type=\"p\",usr=c(0,105),xlab=\"date\",ylab=\"% uptime\",main=\"Web uptime\");
lines(segplot,col=\"blue\",type=\"l\");

# 3,2
query<-paste(\"select date_trunc('day',epochtime)::date as sla_date,(latency*1000.00) as msec from \",arg1,\"_sla_tesla where epochtime between (now() - '\",arg2,\" days'::interval) and (now() - '1 day'::interval) and dnslabel ~ 'web' \",sep=\"\");
segplot<-pg.spi.exec(query);
segplot\$sla_date<-as.Date(segplot\$sla_date);
plot(segplot,col=\"red\",pch=20,type=\"p\",xlab=\"date\",ylab=\"Latency ms\",main=\"Web Latency\");
lines(segplot,col=\"blue\",type=\"l\");

title(paste(\"SLA data for \",arg1,\" \",sep=\"\"),outer=TRUE);

dev.off();

system(paste(\"chmod 644 \",ffilename,sep=\"\"));

par(mfrow=c(1,1));

\$\$ language plr;
" ;

$sth = $dbh->prepare($psqlcmd);

if ( $flags{'verbose'} )
{
	print "\nsth - after prepare\n";
	print Dumper $sth;
}

$sth->execute() || die $DBI::errstr;

if ( $flags{'verbose'} )
{
	print "\nsth - after execute\n";
	print Dumper $sth;
}

$sth->finish();
$dbh->disconnect();

##
## Now call the various functions with all appropriate data for each picture
##
foreach $site (@sites)
{
	foreach $tspan (@tspans)
	{

#
# Produce the plot for this site and tspan
#
		if (( $site eq "us1" ) || ( $site eq "us2" ))
		{
			print "\nPrinting SLA plots for $site and $tspan days \n";

			system("/usr/bin/psql -At -h localhost -U postgres watcher -c \"select sla_plot_us('$site', $tspan)\"") == 0 or die "FATAL ERROR: Could not run function sla_plot_us \n";
		}
		else
		{
			print "\nPrinting SLA plots for $site and $tspan days \n";

			system("/usr/bin/psql -At -h localhost -U postgres watcher -c \"select sla_plot('$site', $tspan)\"") == 0 or die "FATAL ERROR: Could not run function sla_plot \n";
		}
	}
}

##
## End main  -  Begin subroutines
##

#
# Get all command line options into flags hash
#
sub get_opts()
{

   use Getopt::Long qw(:config no_ignore_case bundling);
   Getopt::Long::Configure("bundling");
   GetOptions(
      'debug|d'       => \$flags{'debug'},
      'verbose|v'     => \$flags{'verbose'},
      'graph|g'     => \$flags{'graph'},
      'sync|s'     => \$flags{'sync'},

      'help|usage|h'  => sub {warn &usage; exit 1;})
         or die &usage;

   defined($flags{'debug'}) || ($flags{'debug'} = 0);
   defined($flags{'verbose'}) || ($flags{'verbose'} = 0);
}

# Subroutine:   usage
# Args:         <void>
# Return Value: <void>
# Purpose:      Write the appropriate usage to STDOUT.
sub usage()
{
my $usage = <<EOF;
Usage: $0 [OPTIONS]

    -v, --verbose             Verbose Mode
    -d, --debug               Debug Mode (No DB pull)
    -g, --graph               Only print out graphs
    -h, --help                Print this help
EOF

        print $usage;
}

