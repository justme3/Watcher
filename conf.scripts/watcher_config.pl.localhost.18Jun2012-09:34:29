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
our $CONFIG  = '/home/watcher/watcher.xml';
our $WATCHER  = '/home/watcher';
our $XMLPWD  = '/usr/local/bin/opsadmin/watcher';
our $CONFPWD  = '/home/watcher/conf';

our %flags = ();

our @allargs = @ARGV;

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
my $record_list = ();
my $record = ();
my $xp = ();
my @ips = ();
my $ip = ();
my %seen = ();
my @files = ();
my $file = ();
my $basefile = ();
my $latest = ();
my $dateext = ();
my $cmd = ();

##
## Get backup copies of any changed xml files
##
#
# Get a list of all the sites
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
		$ip=$record->getAttribute("ip");
		push ( @ips, $ip );
	}

  	if(($flags{'debug'}) || ($flags{'verbose'}))
  	{
		print "\nDB source routable ip\n";
		print Dumper \@ips;
  	}

#
# Make the site list unique
#
   undef %seen;
   @ips = grep(!$seen{$_}++, @ips);

   if(($flags{'debug'}) || ($flags{'verbose'}))
   {
      print "\nUnique IPs to look at\n";
      print Dumper \@ips;
   }

#
# For each of the sites
#
	foreach $ip (@ips)
	{
		@files = `/usr/bin/ssh $ip /bin/ls -1 $XMLPWD/*xml`;
		chomp(@files);

   	if(($flags{'debug'}) || ($flags{'verbose'}))
   	{
      	print "\nfiles to backup for $ip \n";
      	print Dumper \@files;
		}

#
# Check each file for differences, if different backup else remove it
#
		foreach $file (@files)
		{
			if($flags{'debug'})
			{
				print "\nfile name to process \n";
				print Dumper $file;
			}

			$basefile = `/bin/basename $file`;
			chomp($basefile);

			if($flags{'debug'})
			{
				print "\nbase name to process \n";
				print Dumper $basefile;
			}

			$cmd= "/bin/ls -1rt $CONFPWD/$basefile.$ip* 2>/dev/null | /usr/bin/tail -1";

			if($flags{'debug'})
			{
				print "\ncommand to run \n";
				print Dumper $cmd;
			}

			$latest = `$cmd`;
			chomp($latest);

			if($flags{'debug'})
			{
				print "\nlatest name to compare to \n";
				print Dumper $latest;
			}

   		if($flags{'verbose'})
   		{
      		print "\n/usr/bin/scp $ip:$file $CONFPWD/$basefile \n";
			}

			system("/usr/bin/scp $ip:$file $CONFPWD/$basefile") == 0 or die "FATAL ERROR: Can't scp file /usr/bin/scp $ip:$file $CONFPWD/$basefile \n";

			if ( $latest eq "" )
			{
				print "\n NO latest FILE FOUND \n";
				$dateext = `/bin/date +%d%b%Y-%H:%M:%S`;
				chomp($dateext);

				system("/bin/mv $CONFPWD/$basefile $CONFPWD/$basefile.$ip.$dateext") == 0 or die "FATAL ERROR: Can't move file /bin/mv $CONFPWD/$basefile $CONFPWD/$basefile.$ip.$dateext :\n";
			}
			else
			{
				if ( `/usr/bin/cmp $CONFPWD/$basefile $latest` )
				{
   				if($flags{'verbose'})
   				{
      				print "\n FILE CHANGED - saving a copy \n";
					}

					$dateext = `/bin/date +%d%b%Y-%H:%M:%S`;
					chomp($dateext);
					system("/bin/mv $CONFPWD/$basefile $CONFPWD/$basefile.$ip.$dateext") == 0 or die "FATAL ERROR: Can't move file /bin/mv $CONFPWD/$basefile $CONFPWD/$basefile.$ip.$dateext :\n";
				}
				else
				{
   				if($flags{'verbose'})
   				{
      				print "\n NO CHANGE - removing scp file \n";
					}

					system("/bin/rm $CONFPWD/$basefile") == 0 or die "FATAL ERROR: Can't remove file /bin/rm $CONFPWD/$basefile :\n";
				}

			}

		}

	}

#
# do all of that for the local files
#
	@files = `/bin/ls -1 /home/watcher/*xml`;
	chomp(@files);

  	if(($flags{'debug'}) || ($flags{'verbose'}))
  	{
     	print "\nfiles to backup for localhost \n";
     	print Dumper \@files;
	}

	foreach $file (@files)
	{
		if($flags{'debug'})
		{
			print "\nfile name to process \n";
			print Dumper $file;
		}

		$basefile = `/bin/basename $file`;
		chomp($basefile);

		if($flags{'debug'})
		{
			print "\nbase name to process \n";
			print Dumper $basefile;
		}

		$cmd= "/bin/ls -1rt /home/watcher/conf/$basefile.localhost* 2>/dev/null | /usr/bin/tail -1";

		if($flags{'debug'})
		{
			print "\ncommand to run \n";
			print Dumper $cmd;
		}

		$latest = `$cmd`;
		chomp($latest);

		if($flags{'debug'})
		{
			print "\nlatest name to compare to \n";
			print Dumper $latest;
		}

		if ( $latest eq "" )
		{
			print "\n NO latest FILE FOUND \n";
			$dateext = `/bin/date +%d%b%Y-%H:%M:%S`;
			chomp($dateext);

			system("/bin/cp /home/watcher/$basefile /home/watcher/conf/$basefile.localhost.$dateext") == 0 or die "FATAL ERROR: Can't copy file /bin/cp /home/watcher/$basefile /home/watcher/conf/$basefile.localhost.$dateext :\n";
		}
		else
		{
			if ( `/usr/bin/cmp /home/watcher/$basefile $latest` )
			{
  				if($flags{'verbose'})
  				{
     				print "\n FILE CHANGED - saving a copy \n";
				}

				$dateext = `/bin/date +%d%b%Y-%H:%M:%S`;
				chomp($dateext);
				system("/bin/cp /home/watcher/$basefile /home/watcher/conf/$basefile.localhost.$dateext") == 0 or die "FATAL ERROR: Can't copy file /bin/cp /home/watcher/$basefile /home/watcher/conf/$basefile.localhost.$dateext :\n";
			}
			else
			{
   			if($flags{'verbose'})
   			{
      			print "\n NO CHANGE - no action on localhost config file \n";
				}

			}

		}

	}

#
# Put in an entry indicating successful completion
#
	$timestamp = time();

	system("/usr/bin/psql -At -h localhost -U postgres watcher -c \"insert into config_time (epochtime) values (to_timestamp($timestamp))\"") == 0 or die "FATAL ERROR: Can't add database entry in config_time \n";

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

