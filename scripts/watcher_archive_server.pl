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
use Storable;

our $AUTHOR     = 'John_Vossler@McAfee.com';
our $VERSION    = 2.0;
our $PODCONFIG  = '/mxl/etc/pod_config.xml';

our %flags = ();

our $start = ();
our $end = ();
our $iteration = ();

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

##
## Create variables for CID processing
##
#
# Create the hashes for CIDs
#
my %timestamp = ();              # epoch seconds

#
# Non hashes
#
my $policydb = ();
my $archivedb = ();
my $cid_counter = ();
my @cids = ();
my $ip = ();
my $cmd = ();
my $insert = ();
my $dbh = ();
my $sth = ();
my $cid = ();
my $xp = ();
my @solrips = ();

#
# Create the server specific hashes
#
my %srv_timestamp = ();
my %srv_type = ();
my %datacenter = ();
my %DNScnt = ();
my %dircnt = ();
my %diskfree = ();

#
# Non hashes for servers
#
my $lastoctet = ();
my $srv_ip = ();
my @diskdf = ();
my $groupid = ();
my $diskdfret = ();
my @masips = ();
my @ips = ();

#
# Define Storage variables
#
my %nfs_timestamp = ();
my %nfsused = ();
my %nfsfree = ();

my $ts = ();
my $index = ();
my $nfsret = ();
my @nfsstat = ();
my @allmpips = ();
my @srvmp = ();
my @fields = ();
my $mp = ();
my $line = ();
my $cidseqnum = ();
my %seen = ();

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
# Get region number
#
   $region = `/mxl/sbin/arregion`;
   chomp($region);

   if(($flags{'debug'}) || ($flags{'verbose'}))
   {
      print "\nRegion number for pair\n";
      print Dumper $region;
   }

#
# Get a list of mas and solr servers from both sites
#
   @masips = @{retrieve("/tmp/watcher_masIPs")};

   @solrips = @{retrieve("/tmp/watcher_solrIPs")};

#
# Get a list of solr servers from both sites
#
   %srv_type = %{retrieve("/tmp/watcher_SRVtype")};

   %datacenter = %{retrieve("/tmp/watcher_DCs")};

	if (( $flags{'debug'} ) || ( $flags{'verbose'} ))
	{
		print "\nmas ip addresses\n";
		print Dumper \@masips;
		print "\nsolr ip addresses\n";
		print Dumper \@solrips;
		print "\nmas ip addresses\n";
		print Dumper \@masips;
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
# Get the list of CIDs
#
   @cids = @{retrieve("/tmp/watcher_CIDs")};

   if(($flags{'debug'}) || ($flags{'verbose'}))
   {
      print "\ncids\n";
      print Dumper \@cids;
      print Dumper $#cids;
   }

   $ts = localtime(time());

	print "###################################################################################### \n";
	print "\n";
	print "Starting new instance at ";
	print Dumper $ts;
	print "\n";
	print "\nStart index = $start   Ending index = $end \n";
	print "\n";
	print "###################################################################################### \n";



##
## Get all server specific data
##
   ###foreach $index ($start..$end)
   ###{
      ###$ip = $datacenter[$index];

	foreach my $ip (keys %datacenter) 
	{
		chomp($ip);

		$srv_timestamp{$ip} = time();

		if (( $flags{'debug'} ) || ( $flags{'verbose'} ))
		{
			print "\nserver timestamp\n";
			print Dumper \%srv_timestamp;
			print "\nserver type\n";
			print Dumper \%srv_type;
		}

		if ( $srv_type{$ip} eq "solr" )
		{

#
# for a solr get the count from DNSdirector
#
			$groupid = `/mxl/sbin/solr --region $region --solr $ip servers | /bin/awk '\$3 ~ hip {print \$1}' hip=$ip`;
			chomp($groupid);

			$DNScnt{$ip} = `/mxl/sbin/solr --region $region --solr $ip shards | /bin/awk '\$4 == gid {print \$6}' gid=$groupid | awk -F\: '\$1 == hip {cnt++} END {print cnt}' hip=$ip`;
			chomp($DNScnt{$ip});

			if (( $flags{'debug'} ) || ( $flags{'verbose'} ))
			{
				print "\nDNS director count\n";
				print Dumper \%DNScnt;
			}

#
# for a solr get the directory count from the server
#
			$dircnt{$ip} = `ssh $ip "su - mxl-archive -c 'ls -ld /mxl/msg_archive/solr/data/[0-9]*[0-9]-[0-9] | wc -l'"`;
			chomp($dircnt{$ip});

			if (( $flags{'debug'} ) || ( $flags{'verbose'} ))
			{
				print "\nDirectory (customer) count\n";
				print Dumper \%dircnt;
			}

#
# for a solr Get the local disk usage for the server
#
			$diskfree{$ip} = `/mxl/sbin/solr --region $region --solr $ip shards | /bin/awk '\$4 == grpid && \$6 ~ hip {print \$7}' grpid=$groupid hip=$ip | /usr/bin/head -1`;
			chomp($diskfree{$ip});

			if (( $flags{'debug'} ) || ( $flags{'verbose'} ))
			{
				print "\nlocal disk free space\n";
				print Dumper \%diskfree;
			}
		}
		else
		{
			$DNScnt{$ip} = 0;
			$dircnt{$ip} = 0;
			$diskfree{$ip} = 0;
		}

	} 	# end server loop

##
## End loop for server data
##

##
## Populate the Archiving tables in the watcher database
##
$cid_counter = 0;

##
## Populate the Server table
##
	foreach $ip (keys %datacenter) 
	{
		$insert = "psql -U postgres -d $dbname -h localhost -c \"insert into arc_server (epochtime,srv_ip,srv_type,datacenter,DNScnt,dircnt,diskfree) values (to_timestamp($srv_timestamp{$ip}),\'$ip\',\'$srv_type{$ip}\',\'$datacenter{$ip}\',$DNScnt{$ip},$dircnt{$ip},$diskfree{$ip})\"";

		if ( $flags{'debug'} ) 
		{			# do not populate the database when debugging
			print "\ninsert command\n";
			print Dumper $insert;
		}
		else
		{
			if ( $flags{'verbose'} )
			{
				print "\ninsert command\n";
				print Dumper $insert;
			}
	     	system("$insert");
		}
	} 	#end write Server information loop

##
## End loop to populate server table
##

##
## Put an entry to mark the last successful completion
##
	if ( ! $flags{'test'} )
	{
		my $lr_timestamp = time();

		$insert = "psql -U postgres -d $dbname -h localhost -c \"insert into arc_server_lastrun (epochtime,thread) values (to_timestamp($lr_timestamp),$iteration)\"";

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
	GetOptions
	(
		'debug|d'       => \$flags{'debug'},
		'test|t'       => \$flags{'test'},
		'verbose|v'     => \$flags{'verbose'},
		'start|s=i'     => \$start,
      'end|e=i'         => \$end,
      'iteration|i=i'     => \$iteration,

		'help|usage|h'  => sub {warn &usage; exit 1;}
	) or die &usage;

	defined($flags{'debug'}) || ($flags{'debug'} = 0);
	defined($flags{'verbose'}) || ($flags{'verbose'} = 0);

	defined($start) || die &usage;
   defined($end) || die &usage;
   defined($iteration) || die &usage;

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

