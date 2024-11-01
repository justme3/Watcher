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
our $VERSION    = 1.0;
our $CONFIG  = '/home/watcher/watcher.xml';
our $WATCHER  = '/home/watcher';

our %flags = ();

our @allargs = @ARGV;

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
## variables
##
my @classes = ();
my $class = ();
my $timestamp = ();
my $xp = ();
my $record = ();
my $record_list = ();
my $name = ();

##
## let the conducting begin
##
#
# Backup the .pl files if any have changed
#
	if ( $flags{'debug'} )
	{
		print "\nCommand line\n";
		print "$WATCHER/watcher_scripts.pl @allargs \n";
	}

	system("$WATCHER/watcher_scripts.pl @allargs") == 0 or die "FATAL ERROR: Can't pull all the perl files :\n";

#
# Backup the xml files if any have changed
#
	if ( $flags{'debug'} )
	{
		print "\nCommand line\n";
		print "$WATCHER/watcher_config.pl @allargs \n";
	}

	system("$WATCHER/watcher_config.pl @allargs") == 0 or die "FATAL ERROR: Can't pull all the config files :\n";

#
# Pull the DB tables
#
	if ( ! $flags{'graph'} )
	{
		if ( $flags{'debug'} )
		{
			print "\nCommand line\n";
			print "$WATCHER/watcher_DBpull.pl @allargs \n";
		}

		system("$WATCHER/watcher_DBpull.pl @allargs") == 0 or die "FATAL ERROR: Can't pull all the databases :\n";

	}

#
# Draw all the pictures
#
	if ( ! $flags{'sync'} )
	{

		$xp = XML::XPath->new(filename => $CONFIG) or die "FATAL ERROR: Can't read $CONFIG as XML:\n";

		if($flags{'debug'})
 		{
			print "\nxp value\n";
			print Dumper $xp;
		}

		$record_list = $xp->find("/watcher/graph/module");

		foreach $record ($record_list->get_nodelist)
		{
			$name=$record->getAttribute("name");
			push ( @classes, $name );
		}

		if($flags{'debug'})
 		{
			print "\nclasses to draw graphs for\n";
			print Dumper \@classes;
		}

		foreach $class (@classes)
		{
			if ( $flags{'debug'} )
			{
				print "\nCommand line\n";
				print "$WATCHER/watcher_$class.pl @allargs \n";
			}

			system("$WATCHER/watcher_$class.pl @allargs") == 0 or die "FATAL ERROR: Can't run graphing module $WATCHER/watcher_$class.pl :\n";

			$timestamp = time();

			system("/usr/bin/psql -At -h localhost -U postgres watcher -c \"insert into graph_time (epochtime,class) values (to_timestamp($timestamp),'$class')\"") == 0 or die "FATAL ERROR: Can't add timestamp in graph_time for class $class.pl :\n";

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
   defined($flags{'graph'}) || ($flags{'graph'} = 0);
   defined($flags{'sync'}) || ($flags{'sync'} = 0);

	if ( $flags{'graph'} && $flags{'sync'} )
	{
		print "\n\nERROR: You cannot specify both --graph and --sync, they are mutually exclusive.\n";
		exit 2;
	}

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
    -d, --debug               Debug Mode 
	 -s, --sync						Only sync the DB, do not output graphs
    -g, --graph               Only print out graphs
    -h, --help                Print this help
EOF

        print $usage;
}

