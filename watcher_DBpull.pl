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
our $VERSION    = 1.1;
our $CONFIG  = '/home/watcher/watcher.xml';

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

##
## variables
##
my $xp = ();
my $record_list = ();
my $record = ();
my $ip = ();
my $table = ();
my $dest = ();
my $site = ();
my $index = ();
my $nindex = ();
my $timestamp = ();
my $dom = ();
my $dow = ();
my $type = ();

##
## Make backup of entire DB
##
$dom = `/bin/date +%m`;
chomp($dom);

$dow = `/bin/date +%w`;
chomp($dow);

if ( $dom == 1 )
{
	$type = "Monthly";
}
else
{
	if ( $dow == 6 )
	{
		$type = "Weekly";
	}
	else
	{
		$type = "Daily";
	}
}

if ( $flags{'verbose'} )
{
	print "\n/usr/bin/pg_dump -U postgres watcher -f /raid/data/backups/watcher-$type.sql\n";
}

system("/usr/bin/pg_dump -U postgres watcher -f /raid/data/backups/watcher-$type.sql");

##
## Start the DB pull
##
#
# Get config information for all arrays
#

   $xp = XML::XPath->new(filename => $CONFIG) or die "FATAL ERROR: Can't read $CONFIG as XML:\n";

   if($flags{'debug'})
   {
      print "\nxp value\n";
      print Dumper $xp;
   }

   $record_list = $xp->find("/watcher/DBpull/table");
   foreach $record ($record_list->get_nodelist)
   {
      $site=$record->getAttribute("site");
      $ip=$record->getAttribute("ip");
      $table=$record->getAttribute("table");
		
   	if(($flags{'debug'}) || ($flags{'verbose'}))
   	{
      	print "\nsite mnemonic\n";
      	print Dumper $site;
			print "\nDB source routable ip\n";
			print Dumper $ip;
      	print "\nDB table name\n";
      	print Dumper $table;
   	}

		$dest = "$site" . "_" . "$table";

		if ( $flags{'verbose'} )
		{
			print "\ndestination table name\n";
			print Dumper $dest;
		}

		if ( $flags{'debug'} )
		{
			print "\n drop table if exists $table\n";
			print "\nssh -n $ip \"/usr/bin/pg_dump -Fc -C -U postgres -t \"$table\" watcher\" | /usr/bin/pg_restore -h localhost -U postgres -d watcher\n";
			print "\ndrop table if exists $dest\n";
			print "\nalter table $table rename to $dest\n";
		}
		else
		{
			system("/usr/bin/psql -At -h localhost -U postgres watcher -c \"drop table if exists $table\"") == 0 or die "FATAL ERROR: Could not drop table $table if exists :\n";
			system("ssh -n $ip \"/usr/bin/pg_dump -Fc -C -U postgres -t \"$table\" watcher\" | /usr/bin/pg_restore -h localhost -U postgres -d watcher") == 0 or die "FATAL ERROR: Could not perform pg_dump for $table on $ip :\n";
			$index = `/usr/bin/psql -At -h localhost -U postgres watcher -c "select indexname from pg_catalog.pg_indexes where tablename='$table'"`;
			$nindex = "$site" . "_" . "$index";

			system("/usr/bin/psql -At -h localhost -U postgres watcher -c \"drop table if exists $dest\"") == 0 or die "FATAL ERROR: Could not drop table $dest if exists \n";
			system("/usr/bin/psql -At -h localhost -U postgres watcher -c \"alter table $table rename to $dest\"") == 0 or die "FATAL ERROR: Could not rename $table to $dest \n";
			system("/usr/bin/psql -At -h localhost -U postgres watcher -c \"alter index $index rename to $nindex\"") == 0 or die "FATAL ERROR: Could not rename index $index to $nindex \n";
		}

		if ( $flags{'verbose'} )
		{
			print "\nindex name\n";
			print Dumper $index;
			print "\nnindex name\n";
			print Dumper $nindex;
		}

		$timestamp = time();

		system("/usr/bin/psql -At -h localhost -U postgres watcher -c \"insert into sync_time (epochtime,site,source_ip,source_table) values (to_timestamp($timestamp),'$site','$ip','$table')\"") == 0 or die "FATAL ERROR: Could not insert into sync_time for $site $ip $table :\n";

	} # End of record loop

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


