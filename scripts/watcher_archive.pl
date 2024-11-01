#!/usr/bin/perl -w

use strict;
use warnings;
use diagnostics;
use Switch;

use lib '/usr/local/bin/opsadmin/perl/';

use DBI;
use Data::Dumper;
use Getopt::Long qw(:config no_ignore_case bundling);
use MXL::Arch;
use MXL::MXL;
use XML::Simple;
use XML::XPath;
use POSIX "sys_wait_h";
use Storable;

our $AUTHOR     = 'John_Vossler@McAfee.com';
our $VERSION    = 2.0;
our $PODCONFIG  = '/mxl/etc/pod_config.xml';

our %flags = ();

our @allargs = @ARGV;

$ENV{'PGPASSWORD'} = 'dbG0d';    # Export the postgres password

sub get_opts();
sub usage();
sub main();

&get_opts();

if($flags{'debug'})
{
	print "flags\n";
	print Dumper(\%flags);
}

##
## Global variables
##
my $pair_dist = ();
my @pair_dist = ();
my $region = ();
my $region_pair = ();
my $pair = ();
my $config = ("/usr/local/bin/opsadmin/watcher/watcher_archive.xml");
my $dbname = ("watcher");
my %seen = ();
my $cid = ();
my $insert = ();
my $dbh = ();
my $sth = ();
my $cidseqnum = ();

#
# Create the server specific hashes
#
my %srv_timestamp = ();
my %srv_type = ();
my %datacenter = ();

#
# Non hashes for servers
#
my $srv_ip = ();
my @masips = ();
my @masips0 = ();
my @masips1 = ();
my @masips2 = ();
my @ips = ();
my @solrips = ();
my @solrips0 = ();
my @solrips1 = ();
my @solrips2 = ();

##
## Create variables for CID processing
##
#
# Non hashes
#
my $lr_timestamp = ();              # epoch seconds
my $ts = ();
my $policydb = ();
my $archivedb = ();
my $cid_counter = ();
my @cids = ();
my @cids1 = ();
my @cids2 = ();
my @cids3 = ();
my $xp = ();
my $array = ();
my $ip_list = ();

#
# Define multithread variables
#
my $start = ();
my $end = ();
my $pid = ();
my $rtnpid = ();
my @pids = ();
my $SRVpid = ();
my $MSpid = ();
my $CIDpid = ();
my $LOOPcnt = ();
my $procsE = ();
my $MSprocs = ();
my $SRVprocs = ();
my $CIDprocs = ();
my $complete = ();

##
## Get the global information needed
##
#
# Get IP of policy database server
#
	$policydb = `/mxl/bin/get_config_entry -f $PODCONFIG -S "pod/db?path"`;
	chomp($policydb);

	if(($flags{'debug'}) || ($flags{'verbose'}))
	{
		print "\npolicy DB IP address\n";
		print Dumper $policydb;
	}

#
# Get IP of the other infra 1 to get the /root/dist files
#
	$xp = XML::XPath->new(filename => $config) or die "FATAL ERROR: Can't read $config as XML:\n";

	if($flags{'debug'})
	{
		print "\nxp value\n";
		print Dumper $xp;
	}

	$ip_list = $xp->find("/configuration/watcher_archive/pair");
	foreach $array ($ip_list->get_nodelist)
	{
		push @pair_dist, $array->getAttribute("ip");
	}

	$procsE = $xp->find("/configuration/processes/MS");
	foreach $array ($procsE->get_nodelist)
	{
		$MSprocs = $array->getAttribute("number");
	}
	chomp($MSprocs);

	$procsE = $xp->find("/configuration/processes/SRV");
	foreach $array ($procsE->get_nodelist)
	{
		$SRVprocs = $array->getAttribute("number");
	}

	$SRVprocs = 1; # server data gathering does not yet support multithreading
	chomp($SRVprocs);

	$procsE = $xp->find("/configuration/processes/CID");
	foreach $array ($procsE->get_nodelist)
	{
		$CIDprocs = $array->getAttribute("number");
	}
	chomp($CIDprocs);

	$region = `/mxl/sbin/arregion`;
	chomp($region);

	if(($flags{'debug'}) || ($flags{'verbose'}))
	{
		print "\nIPs of all the dist files\n";
		print Dumper \@pair_dist;
		print "\nRegion number for pair\n";
		print Dumper $region;
		print "\nNumber of MS processes to spawn\n";
		print Dumper $MSprocs;
		print "\nNumber of SRV processes to spawn\n";
		print Dumper $SRVprocs;
		print "\nNumber of CID processes to spawn\n";
		print Dumper $CIDprocs;
	}

#
# Get a list of mas servers from both sites
#
	open(MAS,"/root/dist/archiving_mas") || die "Cannot open /root/dist/archiving_mas: $!";
	@masips1 = <MAS>;
	close(MAS);

	foreach $pair_dist (@pair_dist)
	{
		@masips0 = `/usr/bin/ssh $pair_dist /bin/cat /root/dist/archiving_mas 2>/dev/null`;
		push ( @masips2, @masips0 );
	}

	@masips = (@masips1, @masips2);
	chomp(@masips);

	foreach $srv_ip (@masips)
	{
		$srv_type{$srv_ip} = "mas";

		@ips = split(/\./,$srv_ip);

		$datacenter{$srv_ip} = "";

		switch ( $ips[1] )
		{
			case 11 { $datacenter{$srv_ip} = "Englewood"; }
			case 12 { $datacenter{$srv_ip} = "Denver"; }
			case 13 { $datacenter{$srv_ip} = "Amsterdam"; }
			case 14 { $datacenter{$srv_ip} = "Tokyo"; }
			case 15 { $datacenter{$srv_ip} = "Sydney"; }
			case 16 { $datacenter{$srv_ip} = "Auckland"; }
			case 17 { $datacenter{$srv_ip} = "London"; }
			case 18 { $datacenter{$srv_ip} = "Hong Kong"; }
			case 19 { $datacenter{$srv_ip} = "Singapore"; }
		}

		if ( $datacenter{$srv_ip} eq "" )
		{
			if ( $ips[2] == 106 )
			{
				$datacenter{$srv_ip} = "Englewood";
			}
			else
			{
				$datacenter{$srv_ip} = "Denver";
			}
		}

	}

#
# Get a list of solr servers from both sites
#
	###open(SOLR,"/root/dist/archiving_solr") || die "Cannot open /root/dist/archiving_solr: $!";
	###@solrips1 = <SOLR>;
	###close(SOLR);

	@solrips1 = `/bin/cat /root/dist/archiving_solr /root/dist/archiving_server 2>/dev/null`;

	foreach $pair_dist (@pair_dist)
	{
		@solrips0 = `/usr/bin/ssh $pair_dist /bin/cat /root/dist/archiving_solr /root/dist/archiving_server 2>/dev/null`;
		push ( @solrips2, @solrips0 );
	}

	@solrips = (@solrips1, @solrips2);
	chomp(@solrips);

	foreach $srv_ip (@solrips)
	{
		chomp($srv_ip);

		$srv_type{$srv_ip} = "solr";

		@ips = split(/\./,$srv_ip);

		$datacenter{$srv_ip} = "";

		switch ( $ips[1] )
		{
			case 11 { $datacenter{$srv_ip} = "Englewood"; }
			case 12 { $datacenter{$srv_ip} = "Denver"; }
			case 13 { $datacenter{$srv_ip} = "Amsterdam"; }
			case 14 { $datacenter{$srv_ip} = "Tokyo"; }
			case 15 { $datacenter{$srv_ip} = "Sydney"; }
			case 16 { $datacenter{$srv_ip} = "Auckland"; }
			case 17 { $datacenter{$srv_ip} = "London"; }
			case 18 { $datacenter{$srv_ip} = "Hong Kong"; }
			case 19 { $datacenter{$srv_ip} = "Singapore"; }
		}

		if ( $datacenter{$srv_ip} eq "" )
		{
			if ( $ips[2] == 106 )
			{
				$datacenter{$srv_ip} = "Englewood";
			}
			else
			{
				$datacenter{$srv_ip} = "Denver";
			}
		}
	}

	if (( $flags{'debug'} ) || ( $flags{'verbose'} ))
	{
		print "\nmas ip addresses\n";
		print Dumper \@masips;
		print "\nsolr ip addresses\n";
		print Dumper \@solrips;
		print "\ndata center\n";
		print Dumper \%datacenter;
		print "\nserver type\n";
		print Dumper \%srv_type;
	}
	
#
# Get IP of archiving database server
#
	$archivedb = `/mxl/bin/get_config_entry -f $PODCONFIG -S "pod/arc_db?path"`;
	chomp($archivedb);

	if(($flags{'debug'}) || ($flags{'verbose'}))
	{
		print "\narchive DB IP address\n";
		print Dumper $archivedb;
	}

#
# Get the list of CIDs from the DB server and from DNSdirector
#
	@cids1 = Arch::cids_from_mail_source();
	chomp(@cids1);

	if(($flags{'debug'}) || ($flags{'verbose'}))
	{
		print "\ncids1 (from Arch::cids_from_mail_source()) for validation\n";
		print Dumper \@cids1;
	}

	@cids2 = `/usr/bin/psql -At -h $archivedb -U postgres mxl_archive -c "select distinct(id) from arc_cust_settings"`;
	###@cids2 = `/mxl/sbin/dnsdirector solr list cid --region $region `;
	chomp(@cids2);

	if(($flags{'debug'}) || ($flags{'verbose'}))
	{
		print "\ncids2 (from mxl_archive DB ) for validation\n";
		print Dumper \@cids2;
	}

	if( $flags{'test'} )
	{
		###@cids3 = (@cids1[0..4], @cids2[0..4]);
		@cids3 = qw(32455077 71811741 18532915 34234206);  # pod 2
		###@cids3 = qw(2084561595 2086430767 2081204722);  # pod 1
		chomp(@cids3);
	}
	else
	{
		@cids3 = (@cids1, @cids2);
		chomp(@cids3);
	}

	undef %seen;
	@cids3 = grep(!$seen{$_}++, @cids3);

	if(($flags{'debug'}) || ($flags{'verbose'}))
	{
		print "\ncids3 for validation\n";
		print Dumper \@cids3;
	}

#
# Validate all the CIDs and eliminate any that have not ingested any mail
#
	@cids = ();

	foreach $cid (@cids3) 
	{
		$cidseqnum = `/mxl/sbin/arseqcnt $cid`;

		if ( ( $cidseqnum > 0 ) && ( `/mxl/sbin/dnsdir-cust $cid solr` ne "(undefined)" ) && ( `/mxl/sbin/dnsdir-cust $cid ingest.mas` ne "(undefined)" ) && ( `/mxl/sbin/dnsdir-cust $cid search.mas` ne "(undefined)" ) )
		{
			push ( @cids, $cid );
		}
	}

	chomp(@cids);

	$cid_counter = 0;

	if(($flags{'debug'}) || ($flags{'verbose'}))
	{
		print "\ncids\n";
		print Dumper \@cids;
		print Dumper $#cids;
	}

#
# If the number of CIDs are less than 50 the thread counts should be 1
#
	if ( $#cids < 50 )
	{
		$MSprocs = 1;
		chomp($MSprocs);

		$SRVprocs = 1;
		chomp($SRVprocs);

		$CIDprocs = 1;
		chomp($CIDprocs);
		
	}

	$ts = localtime(time());

	print "###################################################################################### \n";
	print "\n";
	print "Starting new instance at ";
	print Dumper $ts;
	print "\n";
	print "###################################################################################### \n";

##
## Write out the arrays that are needed by the children
##
#
# CIDs array
#
	store(\@cids, "/tmp/watcher_CIDs");

#
# mas IPs array
#
	store(\@masips, "/tmp/watcher_masIPs");

#
# solr IPs array
#
	store(\@solrips, "/tmp/watcher_solrIPs");

#
# Datacenter hash
#
store(\%datacenter, "/tmp/watcher_DCs");

#
# Server type hash
#
store(\%srv_type, "/tmp/watcher_SRVtype");

##
## Spawn off Mail Source processing
##
	$start = 0;
	$end = 0;
	$complete = $#cids;
	$LOOPcnt = 0;	

	if($flags{'debug'})
	{
		print "\nInitial-Loop MS $LOOPcnt, Procs = $MSprocs, Start = $start, End = $end, Complete = $complete \n";
	}

	while ( $end < $complete )
	{
		$LOOPcnt += 1;
		$end = int(($complete/$MSprocs)*$LOOPcnt);

		if($flags{'debug'})
		{
			print "\nPre-Loop MS $LOOPcnt, Start = $start, End = $end \n";
		}


		if (( abs( $complete - $end ) < 20 ) && ( $complete != $end ))
		{
			$end = $complete;
		}

		if($flags{'debug'})
		{
			print "\nPost-Loop MS $LOOPcnt, Start = $start, End = $end \n";
		}

		$MSpid = fork();
		chomp($MSpid);

		push ( @pids, $MSpid );
		chomp(@pids);

		if(($flags{'debug'}) || ($flags{'verbose'}))
		{
			print "\nMail Source child $MSpid \n";
		}

		if (( defined($MSpid) ) && ( $MSpid==0 ))
		{
			exec("/usr/local/bin/opsadmin/watcher/watcher_archive_mailsource.pl @allargs --start $start --end $end --iteration $LOOPcnt 1>>/var/log/mxl/watcher/mailsource-$LOOPcnt.log 2>>/var/log/mxl/watcher/mailsource-$LOOPcnt.log");

			$ts = localtime(time());
			chomp($ts);
	
			print "\nMS processing exited abnormally at $ts \n";
			exit(0); 
		}
	
		$start = $end + 1;

		if($flags{'debug'})
		{
			print "\nEnd-Loop MS $LOOPcnt, Start = $start, End = $end \n";
		}
	}

##
## Spawn off Server processing
##
	$start = 0;
	$end = 0;
	$complete = $#cids;
	$LOOPcnt = 0;	

	if($flags{'debug'})
	{
		print "\nInitial-Loop SRV $LOOPcnt, Procs = $SRVprocs, Start = $start, End = $end, Complete = $complete \n";
	}

	while ( $end < $complete )
	{
		$LOOPcnt += 1;
		$end = int(($complete/$SRVprocs)*$LOOPcnt);

		if($flags{'debug'})
		{
			print "\nPre-Loop SRV $LOOPcnt, Start = $start, End = $end \n";
		}


		if (( abs( $complete - $end ) < 20 ) && ( $complete != $end ))
		{
			$end = $complete;
		}

		if($flags{'debug'})
		{
			print "\nPost-Loop SRV $LOOPcnt, Start = $start, End = $end \n";
		}

		$SRVpid = fork();
		chomp($SRVpid);

		push ( @pids, $SRVpid );
		chomp(@pids);

		if(($flags{'debug'}) || ($flags{'verbose'}))
		{
			print "\nServer child $SRVpid \n";
		}

		if (( defined($SRVpid) ) && ( $SRVpid==0 ))
		{
			exec("/usr/local/bin/opsadmin/watcher/watcher_archive_server.pl @allargs --start $start --end $end --iteration $LOOPcnt 1>>/var/log/mxl/watcher/server-$LOOPcnt.log 2>>/var/log/mxl/watcher/server-$LOOPcnt.log");

			$ts = localtime(time());
			chomp($ts);

			print "\nServer processing exited abnormally at $ts \n";
			exit(0); 
		}
	
		$start = $end + 1;

		if($flags{'debug'})
		{
			print "\nEnd-Loop SRV $LOOPcnt, Start = $start, End = $end \n";
		}
	}

##
## Spawn off CID processing based on the number of parallel procs to run
##
	$start = 0;
	$end = 0;
	$complete = $#cids;
	$LOOPcnt = 0;	

	if($flags{'debug'})
	{
		print "\nInitial-Loop $LOOPcnt, Procs = $CIDprocs, Start = $start, End = $end, Complete = $complete \n";
	}

	while ( $end < $complete )
	{
		$LOOPcnt += 1;
		$end = int(($complete/$CIDprocs)*$LOOPcnt);

		if($flags{'debug'})
		{
			print "\nPre-Loop $LOOPcnt, Start = $start, End = $end \n";
		}


		if (( abs( $complete - $end ) < 20 ) && ( $complete != $end ))
		{
			$end = $complete;
		}

		if($flags{'debug'})
		{
			print "\nPost-Loop $LOOPcnt, Start = $start, End = $end \n";
		}

		$CIDpid = fork();
		chomp($CIDpid);

		push ( @pids, $CIDpid );
		chomp(@pids);
	
		if (( defined($CIDpid) ) && ($CIDpid==0 ))
		{
			exec("/usr/local/bin/opsadmin/watcher/watcher_archive_cid.pl @allargs --start $start --end $end --iteration $LOOPcnt 1>>/var/log/mxl/watcher/cid-$LOOPcnt.log 2>>/var/log/mxl/watcher/cid-$LOOPcnt.log");

			$ts = localtime(time());
			chomp($ts);

			print "\nCID process $LOOPcnt processing exited abnormally at $ts \n";
			exit(0);
		}
	
		$start = $end + 1;

		if($flags{'debug'})
		{
			print "\nEnd-Loop $LOOPcnt, Start = $start, End = $end \n";
		}
	}

##
## Wait until all the Child processes are complete
##
foreach $pid (@pids)
{
	$ts = localtime(time());
	chomp($ts);

	$rtnpid = waitpid($pid, 0);

	print "\nPID $pid, rtnpid $rtnpid, completed with return code $? at $ts \n";
}

##
## Put an entry to mark the last successful completion
##
	if ( ! $flags{'test'} )
	{
		my $lr_timestamp = time();

		$insert = "psql -U postgres -d $dbname -h localhost -c \"insert into arc_lastrun (epochtime) values (to_timestamp($lr_timestamp))\"";

		if ( $flags{'debug'} ) 
		{			# do not populate the database when debugging
			print "\narc_lastrun insert command\n";
			print Dumper $insert;
		}
		else
		{
			if ( $flags{'verbose'} )
			{
				print "\narc_lastrun insert command\n";
				print Dumper $insert;
			}
			print "\narc_lastrun insert \n";
    		system("$insert");
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
		'test|t'       => \$flags{'test'},
		'verbose|v'     => \$flags{'verbose'},

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
    -d, --debug               Debug Mode (limited CIDs no DB writes)
    -t, --test                Test Mode (limited CIDs)
	 -s, --start               Start subscript
    -e, --end                 Ending subscript
    -i, --iteration           Thread number
    -h, --help                Print this help
EOF

        print $usage;
}

