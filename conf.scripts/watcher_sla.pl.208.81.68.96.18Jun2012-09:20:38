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
my $config = ("/usr/local/bin/opsadmin/watcher/watcher_sla.xml");

##
## Create variables for CID processing
##
#
# variables
#
my $timestamp = ();				# epoch seconds
my $ts = ();						# epoch seconds
my @DNSlabels = ();
my $DNSlabel = ();
my $filedate = ();
my $filepolls = ();
my $filesuccess = ();
my $filelatency = ();
my $xp = ();
my @ip_list = ();
my $label_list = ();
my $array = ();
my $label_array = ();
my $ip = ();
my @records = ();
my $record = ();
my $year = ();
my $month = ();
my $day = ();
my @line = ();
my $sth = ();
my $dbh = ();
my $insert = ();
my $ip_list = ();

##
## Get the global information needed
##
#
# Put header into log file
#
$ts = localtime(time());

print "###################################################################################### \n";
print "\n";
print "Starting new instance at $ts \n";
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

	$ip_list = $xp->find("/configuration/tesla/source");
	foreach $array ($ip_list->get_nodelist)
	{
		$ip = $array->getAttribute("ip");
		chomp($ip);

		if(($flags{'debug'}) || ($flags{'verbose'}))
		{
			print "\nTESLA source ip\n";
			print Dumper $ip;
		}


		$label_list = $xp->find("/configuration/service/name");
		foreach $label_array ($label_list->get_nodelist)
		{
			$DNSlabel = $label_array->getAttribute("totals");
			chomp($DNSlabel);
	
			if(($flags{'debug'}) || ($flags{'verbose'}))
			{
				print "\nTESLA source service\n";
				print Dumper $DNSlabel;
			}

			@records = `/usr/bin/ssh $ip /bin/cat /usr/local/TESLA/logs/totals/$DNSlabel`;

			foreach $record (@records)
			{
				@line = split(/\s+/,$record);

				if(($flags{'debug'}) || ($flags{'verbose'}))
				{
					print "\nRecord to process\n";
					print Dumper \@line;
				}

				$filedate = $line[0];

				if ( defined $line[1] )
				{
					$filepolls = $line[1];
				}
				else
				{
					$filepolls = 0;
				}

				if ( defined $line[2] )
				{
					$filesuccess = $line[2];
				}
				else
				{
					$filesuccess = 0;
				}

				if ( defined $line[3] )
				{
					$filelatency = $line[3];
				}
				else
				{
					$filelatency = 0;
				}

				$year = substr $filedate,0,4;
				$month =  substr $filedate,4,2;
				$day = substr $filedate,6,2;

				$timestamp = "$year" . "-" . "$month" . "-" . "$day" . " 24:00:00-06";

				$dbh = DBI->connect("dbi:Pg:dbname=watcher;host=localhost","postgres","") || die $DBI::errstr;

				$insert =  "insert into sla_tesla (epochtime,dnslabel,polls,successes,latency) values (?,?,?,?,?)";

				$sth = $dbh->prepare($insert);

				if ( $flags{'debug'} )
				{        # do not populate the database when debugging
					print "\ninsert command\n";
					print Dumper $insert;
					print "execute($timestamp,$DNSlabel,$filepolls,$filesuccess,$filelatency)";
				}
				else
				{
					if ( $flags{'verbose'} )
					{
						print "\ninsert command\n";
						print Dumper $insert;
						print Dumper $dbh;
						print Dumper $sth;
						print "execute($timestamp,$DNSlabel,$filepolls,$filesuccess,$filelatency)";
					}

					if ( `/usr/bin/psql -At -U postgres watcher -c "select count(*) from sla_tesla where epochtime = \'$timestamp\' and dnslabel = \'$DNSlabel\'"` <= 0 )
					{
						print "\n Adding line: $timestamp $DNSlabel $filepolls $filesuccess $filelatency \n";
						$sth->execute($timestamp,$DNSlabel,$filepolls,$filesuccess,$filelatency) || die $DBI::errstr;
						$sth->finish();
						$dbh->disconnect();
					}

				}

			}

		}

	}

##
## End processing loop
##

##
## Put an entry to mark the last successful completion
##
	my $lr_timestamp = time();

	$insert = "psql -U postgres -d watcher -h localhost -c \"insert into sla_lastrun (epochtime) values (to_timestamp($lr_timestamp))\"";

	if ( $flags{'debug'} )
	{        # do not populate the database when debugging
  		print "\nsla_lastrun insert command\n";
  		print Dumper $insert;
  	}
  	else
  	{
  		if ( $flags{'verbose'} )
  		{
  			print "\nsla_lastrun insert command\n";
  			print Dumper $insert;
  		}
  		system("$insert");
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

