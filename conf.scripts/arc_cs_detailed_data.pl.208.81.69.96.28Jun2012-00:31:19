#!/usr/bin/perl -w

use strict;
use warnings;
use diagnostics;

use lib '/usr/local/bin/opsadmin/perl/';

use DBI;
use Data::Dumper;
use Switch;
use Getopt::Long qw(:config no_ignore_case bundling);
use MXL::Arch;
use MXL::MXL;
use XML::Simple;

require "ctime.pl";

our $AUTHOR     = 'John_Vossler@McAfee.com';
our $VERSION    = 0.9;
our $DESTINATION  = '/home/www/html/cs/arc_data';
our $MAXDETAILS  = 100;

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
my $cid = ();
my @allcids = ();
my $region = ();
my $seqnum = ();
my $timestamp = ();


##
## Get the global information needed
##
$region = `/mxl/sbin/arregion`;
chomp($region);

#
# Get the list of all CIDs
#
@allcids=`/usr/bin/psql -At -U postgres watcher -c "select distinct(cid) from arc_cid where epochtime between (now() - '24 hours'::interval) and (now())"`;
chomp(@allcids);

foreach $cid (@allcids)
{
	$timestamp = localtime(time());
	chomp($timestamp);

	$seqnum = `/mxl/sbin/arseqcnt $cid`;
	chomp($seqnum);

	if (($flags{'debug'}) || ($flags{'verbose'}))
	{
		print "\nCID = $cid \n";
		print "Timestamp = $timestamp \n";
   	print "Sequence # = $seqnum\n";
	}

	system("echo \" $timestamp    $seqnum \" >> $DESTINATION/$cid");
	system("echo \" Time                       Sequence Number                CID=$cid \" > $DESTINATION/$cid.trim");
	system("grep -v Time $DESTINATION/$cid | tail -$MAXDETAILS >> $DESTINATION/$cid.trim");
	system("mv $DESTINATION/$cid.trim $DESTINATION/$cid");
	system("chmod 0644 $DESTINATION/$cid");

}


##
## Get all command line options into flags hash
##
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

