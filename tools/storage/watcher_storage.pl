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

our $AUTHOR     = 'John_Vossler@McAfee.com';
our $VERSION    = 1.0;

our %flags = ();

$ENV{'PGPASSWORD'} = 'dbG0d';    # Export the postgres password

sub get_opts();
sub usage();

&get_opts();

if($flags{'debug'})
{
	print "flags\n";
	print Dumper(\%flags);
}

##
## Global variables
##
my $config = ("/usr/local/bin/opsadmin/watcher/watcher_storage.xml");

##
## Create variables for CID processing
##
#
# variables
#
my $timestamp = ();				# epoch seconds
my $ssc_fslist = ();
my $ssc_fsdetail = ();
my $fsdetail = ();
my $ts = ();						# epoch seconds
my %password = ();
my %smu_user = ();
my %ip = ();
my @arrayKeys = ();
my $fs_usage = ();
my $ss_usage = ();
my @fslist = ();
my $sth = ();
my $dbh = ();
my $insert = ();
my $xp = ();
my $array = ();
my @fields = ();
my $array_list = ();
my $bacluster = ();
my $fs = ();

##
## Get the global information needed
##
#
# Put header into log file
#
	$ts = localtime(time());

	print "###################################################################################### \n";
	print "\n";
	print "Starting new instance at ";
	print Dumper $ts;
	print "\n";
	print "###################################################################################### \n";

#
# Get config information for all arrays
#

	$xp = XML::XPath->new(filename => $config) or die "FATAL ERROR: Can't read $config as XML:\n";

	if($flags{'debug'})
	{
		print "\nxp value\n";
		print Dumper $xp;
	}

	$array_list = $xp->find("/configuration/bluearc/array");
	foreach $array ($array_list->get_nodelist)
	{
		push @arrayKeys, $array->getAttribute("name");
		$smu_user{$array->getAttribute("name")}=$array->getAttribute("user");
		$password{$array->getAttribute("name")}=$array->getAttribute("password");
		$ip{$array->getAttribute("name")}=$array->getAttribute("smu_ip");
	}

	if(($flags{'debug'}) || ($flags{'verbose'}))
	{
		print "\nSMU names\n";
		print Dumper \@arrayKeys;
		print "\nSMU users\n";
		print Dumper \%smu_user;
		print "\nSMU passwords\n";
		print Dumper \%password;
		print "\nArray IPs\n";
		print Dumper \%ip;
	}

#
# Cycle through the input records and process all file systems
#
	foreach $bacluster (@arrayKeys)
	{

		if ( $flags{'verbose'} )
		{
			print "\nSMU user\n";
			print Dumper $smu_user{$bacluster};
			print "\nSMU user password\n";
			print Dumper $password{$bacluster};
			print "\nsite name\n";
			print Dumper $bacluster;
			print "\nIP address\n";
			print Dumper $ip{$bacluster};
		}

#
# Get a list of file systems from this BA array
#
		$ssc_fslist = "ssh manager\@$ip{$bacluster} \'ssc -u $smu_user{$bacluster} -p $password{$bacluster} 192.0.2.2 df\' | grep \'%\' | awk \'{print \$2}\'";

		if($flags{'debug'}) 
		{
			print "$ssc_fslist\n";
		}

		@fslist = `$ssc_fslist`;
		chomp(@fslist);

		if(($flags{'debug'}) || ($flags{'verbose'}))
		{
			print "\nFile system list\n";
			print Dumper \@fslist;
		}

#
# For each file system on this array get the appropriate information
#  populate that information into the local watcher DB
#
		foreach $fs (@fslist)
		{
			$timestamp = time();

			$ssc_fsdetail = "ssh manager\@$ip{$bacluster} \'ssc -u $smu_user{$bacluster} -p $password{$bacluster} 192.0.2.2 df\' | grep $fs";

			if($flags{'debug'}) 
			{
				print "$ssc_fsdetail\n";
			}

			$fsdetail = `$ssc_fsdetail`;
			chomp($fsdetail);

			@fields = split('\s+', $fsdetail);
			$fs_usage = $fields[7];
			$ss_usage = $fields[10];

			$fs_usage =~ s/\(//; $fs_usage =~ s/\)//; $fs_usage =~ s/\%//;
			$ss_usage =~ s/\(//; $ss_usage =~ s/\)//; $ss_usage =~ s/\%//;

			if($flags{'debug'}) 
			{
				print "$fs used: $fs_usage\%\n";
				print "$fs snapshot used: $ss_usage\%\n";
			}

#
# Populate the DB
#
			$dbh = DBI->connect("dbi:Pg:dbname=watcher;host=localhost","postgres","") || die $DBI::errstr;

			$insert =  "insert into ba_storage (epochtime,baname,ba_ip,fsname,percent_used,percent_snapshot) values (to_timestamp(?),?,?,?,?,?)";

			$sth = $dbh->prepare($insert);

			if ( $flags{'debug'} )
			{        # do not populate the database when debugging
				print "\ninsert command\n";
				print Dumper $insert;
				print "execute($timestamp,$bacluster,$ip{$bacluster},$fs,$fs_usage,$ss_usage) \n";
			}
			else
			{
				if ( $flags{'verbose'} )
				{
					print "\ninsert command\n";
					print Dumper $insert;
					print Dumper $dbh;
					print Dumper $sth;
					print "execute($timestamp,$bacluster,$ip{$bacluster},$fs,$fs_usage,$ss_usage) \n";
				}

  
				$sth->execute($timestamp,$bacluster,$ip{$bacluster},$fs,$fs_usage,$ss_usage) || die $DBI::errstr;

				$sth->finish();
				$dbh->disconnect();
         }

		}   # end fs list

	}   # end array list

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
    -h, --help                Print this help
EOF

        print $usage;
}

