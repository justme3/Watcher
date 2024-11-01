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

our $AUTHOR     = 'John_Vossler@McAfee.com';
our $VERSION    = 0.8;
our $PODCONFIG  = '/mxl/etc/pod_config.xml';

our %flags = ();

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

##
## Create variables for CID processing
##
#
# Create the hashes for CIDs
#
my %timestamp = ();              # epoch seconds
my %severe1 = ();
my %severe2 = ();
my %solrcnt1 = ();
my %solrcnt2 = ();
my %solrip = ();
my %cstnam = ();
my %solrlabel = ();
my %solrseg1 = ();
my %solrseg2 = ();
my %solrej1 = ();
my %solrej2 = ();
my %sizemb1 = ();
my %sizemb2 = ();
my %masrej = ();
my %mscnt  = ();
my %usrcnt = ();
my %solrndxbklg = ();
my %solrseqnum = ();
my %ndx24 = ();
my %ingest24 = ();
my %segment = ();

#
# Create hashes for SIDs
#
my %ms_timestamp = ();
my %ms_status = ();
my %ms_host = ();
my %ms_user = ();
my %ms_port = ();
my %ms_type = ();
my %ms_name = ();
my %ms_desc = ();
my %ms_backlog = ();
my %ms_ingest24 = ();

#
# Non hashes
#
my $policydb = ();
my $archivedb = ();
my $solrndxseqn = ();
my $count = ();
my $results = ();
my $cid_counter = ();
my $debug_cid = 20;
my @cids = ();
my @cids1 = ();
my @cids2 = ();
my @cids3 = ();
my $ip = ();
my @ipsolr = ();
my @ndx = ();
my @solr = ();
my $indexstr = ();
my @indexcnt = ();
my @mailers = ();
my @sqlmailers = ();
my $ms_ref = ();
my %ms_hash = ();
my $cmd = ();
my $ndxseqstr = ();
my @ndxseq = ();
my $insert = ();
my $dbh = ();
my $sth = ();
my $cid = ();
my @ingestmaspool = ();
my @searchmaspool = ();
my @maspool = ();
my @size = ();
my @solrips = ();
my @solrips1 = ();
my @solrips2 = ();

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
my $DNSret = ();
my @DNSlabel = ();
my $diskdfret = ();
my @masips = ();
my @masips1 = ();
my @masips2 = ();
my @ips = ();

#
# Define Storage variables
#
my %nfs_timestamp = ();
my %nfsused = ();
my %nfsfree = ();

my $nfsret = ();
my @nfsstat = ();
my @nfsmp = ();
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
	open(PDB,"/root/dist/db.policy") || die "Cannot open /root/dist/db.policy: $!";
	$policydb = <PDB>;
	close(PDB);

	chomp($policydb);

#
# Get IP of the other infra 1 to get the /root/dist files
#
	open(PDIST,"/home/jvossler/watcher/watcher_archive.conf") || die "Cannot open /home/jvossler/watcher/watcher_archive.conf: $!";
	$pair_dist = <PDIST>;
	close(PDIST);

	chomp($pair_dist);

	if(($flags{'debug'}) || ($flags{'verbose'}))
	{
		print "\nfile inputs\n";
		print Dumper $policydb;
		print Dumper $pair_dist;
	}

#
# Get a list of mas servers from both sites
#
	open(MAS,"/root/dist/archiving_mas") || die "Cannot open /root/dist/archiving_mas: $!";
	@masips1 = <MAS>;
	close(MAS);

	@masips2 = `/usr/bin/ssh $pair_dist /bin/cat /root/dist/archiving_mas`;

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
			case 18 { $datacenter{$srv_ip} = "London"; }
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
	open(SOLR,"/root/dist/archiving_solr") || die "Cannot open /root/dist/archiving_solr: $!";
	@solrips1 = <SOLR>;
	close(SOLR);

	@solrips2 = `/usr/bin/ssh $pair_dist /bin/cat /root/dist/archiving_solr`;

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
			case 18 { $datacenter{$srv_ip} = "London"; }
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
	$archivedb = `/mxl/bin/get_config_entry -f $PODCONFIG -S "pod/arc_grid_db?path"`;
	chomp($archivedb);

	if(($flags{'debug'}) || ($flags{'verbose'}))
	{
		print "\narchive DB IP address\n";
		print Dumper $archivedb;
	}

##
#
# Get the list of CIDs from the DB server
#
	if( $flags{'test'} )
	{
		if ( $pair_dist =~ /10.1.107.1/ )
		{
			# pod 1
			@cids3 = qw ( 2004755105 2012820981 2071307165 2078345417 2084509171 2076577354 );
		}
		else
		{
			# pod 2
			@cids3 = qw ( 33506819 44678636 33358612 28731319 );
		}
	}
	else
	{
		@cids1 = Arch::cids_from_mail_source();
		chomp(@cids1);

		@cids2 = `/mxl/sbin/dnsdirector solr list cid`;
		chomp(@cids2);

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

		if ( $cidseqnum > 0 )
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

##
## Get data and populate the CID table
##
	foreach $cid (@cids) 
	{
		$cid_counter++;

#
# Get number of messages ingested in last 24 hours
#
      @mailers = Arch::sids_from_cid($cid);

		if ( $#mailers > 0 )
		{
			@sqlmailers = join(', ', @mailers);
		}
		else
		{
			@sqlmailers = @mailers;
		}

		if (( $flags{'debug'} ) || ( $flags{'verbose'} ))
		{
			print "\nCID\n";
			print Dumper $cid;
			print "\nmailers\n";
			print Dumper \@mailers;
			print "\nSQL mailers\n";
			print Dumper \@sqlmailers;
		}


	}  # end of cid loop

##
## End CID loop
##

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
    -h, --help                Print this help
EOF

        print $usage;
}

