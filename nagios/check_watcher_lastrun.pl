#!/usr/bin/perl
# File: check_watcher_lastrun.pl
# Desc: Checks the lastrun date for any watcher tables on this host
# Code: by John Vossler Dec 2011

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
our $VERSION    = 1.0;
our $WATCHER  = '/home/watcher';
our $WARN  = 86400;  # 24 hours
our $CRIT  = 129600;  # 36 hours

our %flags = ();

sub get_opts();
sub usage();

&get_opts();

if($flags{'debug'})
{
   print "\nflags\n";
   print Dumper(\%flags);
}

##
## variables
##
my $currenttime = ();
my $table = ();
my @tables = ();
my @localtables = ();
my @gorttables = ();
my $lastsuccess = ();
my $difftime = ();
my $status = ();
my $return = ();
my $output = ();
my $hours = ();

#
# Set the current time
#
$currenttime = time();

$return = 0;

##
## Get a list of all the "lastrun" tables
##
@localtables = `/usr/bin/psql -At -Upostgres watcher -c "select tablename from pg_tables where tablename ~ 'lastrun'"`;
chomp(@localtables);

@gorttables = `/usr/bin/psql -At -Upostgres watcher -c "select tablename from pg_tables where tablename ~ '_time'"`;
chomp(@gorttables);

@tables = (@localtables,@gorttables);
chomp(@tables);

if ( $flags{'debug'} )
{
	print "\nTables to Check \n";
	print Dumper @tables;
}
	
foreach $table (@tables)
{

	$lastsuccess = `/usr/bin/psql -At -Upostgres watcher -c "select date_part('epoch',epochtime)::int from $table order by epochtime desc limit 1"`;
	chomp($lastsuccess);

	if ( $lastsuccess eq "" )
	{
		$lastsuccess = $currenttime;
	}

	if ( $flags{'debug'} )
	{
		print "\ncurrent time \n";
		print Dumper $currenttime;
		print "\ntable time $table ";
		print Dumper $lastsuccess;
	}

	$difftime = $currenttime - $lastsuccess;
	chomp($difftime);

	if ( $flags{'debug'} )
	{
		print "\ndiff time \n";
		print Dumper $difftime;
	}

	if ( $difftime < $WARN )
	{
		$status = "O.K.";
	}
	else
	{
		if ( $difftime < $CRIT )
		{
			$status = "WARN";
			
			if ( $return == 0 )
			{
				$return = 1;
			}
		}
		else
		{
			$status = "CRIT";
			
			$return = 2;
		}
	}

if ( $status ne "O.K." )
{
	$hours = sprintf("%.2f",$difftime / 3600);
	$output .= "$status - $table is $hours hours behind<Br>";
}

}

if ( $return == 0 )
{
	$output = "0.K.";
}

$output =~ s/<Br>$//;
print "$output\n";

if ( $flags{'verbose'} )
{
	print "\nReturn code ";
	print Dumper $return;
}

exit $return;


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
    -d, --debug               Debug Mode (No DB pull)
EOF

        print $usage;
}

