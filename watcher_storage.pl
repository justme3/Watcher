#!/usr/bin/perl -w

use strict;
use warnings;
use diagnostics;
use Switch;
use POSIX;

use lib '/usr/local/bin/opsadmin/perl/';

use DBI;
use Data::Dumper;
use Getopt::Long qw(:config no_ignore_case bundling);
use XML::Simple;
use XML::XPath;

###require "ctime.pl";

our $AUTHOR     = 'John_Vossler@McAfee.com';
our $VERSION    = 1.0;

our %flags = ();

$ENV{'PGPASSWORD'} = 'dbG0d';    # Export the postgres password

sub get_opts();
sub rxc();
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
my @tspans = (30,60,90,180);

##
## variables
##
my $site = ();
my @sites = ();
my @mps = ();
my %seen = ();
my $msize = ();
my $timestamp = ();
my $nplots = ();
my $numplot = ();
my $tspan = ();
my $dest = ();
my $sth = ();
my $dbh = ();
my $psqlcmd = ();


##
## Get config information for which sites to draw pretty pictures for from table names in DB
##
#
# Get each domain for each class of Storage data
#
	@sites = `/usr/bin/psql -At -U postgres watcher -c "select relname from pg_class where relname ~ '_ba_storage'" | awk -F\_ '{print \$1}'`;
	chomp(@sites);

#
# Make the list of sites unique
#
	undef %seen;
   @sites = grep(!$seen{$_}++, @sites);
	chomp(@sites);

	if ( $flags{'verbose'} )
	{
		print "\nlist of sites\n";
		print Dumper \@sites;
	}

##
## Create all the functions we will need for graph creation
##
#
# define graphs for the Storage array plots for a given site
#
$dbh = DBI->connect("dbi:Pg:dbname=watcher;host=localhost","postgres","") || die $DBI::errstr;

if ( $flags{'verbose'} )
{
	print "\nconnect status in dbh\n";
	print Dumper $dbh;
}

		                         #select storage_plot('$site', $tspan,     '$msize'        nplots)
		                         #select storage_plot( arg1  , arg2   , arg3   ,  arg4  ,   arg5 )
$psqlcmd = "create or replace function storage_plot(varchar, integer, integer, integer, integer) returns void as \$\$

filename<-paste(\"\",arg1,\"-storage-\",arg2,\".png\",sep=\"\");
ffilename<-paste(\"\",\"var\",\"www\",\"html\",\"watcher\",\"storage\",filename,sep=\"/\");
png(ffilename,width = 1200,height = 1000,units = \"px\",pointsize = 16);

gstart<-format((Sys.time() - ((3600*24)*arg2)), \"%Y-%m-%d\");
gend<-format(Sys.time(), \"%Y-%m-%d\");

dlabels<-c();
for ( x in (arg2:0))
{
   lbint<-c(format(Sys.time() - (((3600*24)*arg2)*(x/arg2)), \"%m-%d\"));
   dlabels<-c(c(dlabels), c(lbint));
}

par(mfrow=c(arg3,arg4),oma=c(5,0,3,0),cex.main=2);

mindex<-arg5 - 1 ;
for (mpindex in 0:mindex) 
{
	fquery<-paste(\"select distinct(fsname) from \",arg1,\"_ba_storage where epochtime between (now() - '\",arg2,\" days'::interval) and (now() - '1 day'::interval) order by fsname asc offset \",mpindex,\" limit 1 \",sep=\"\");
	fsnam<-data.frame(pg.spi.exec(fquery));
###pg.thrownotice(fsnam);
	query<-paste(\"select date_trunc('day',epochtime),percent_used from \",arg1,\"_ba_storage where fsname = '\",fsnam,\"' and epochtime between (now() - '\",arg2,\" days'::interval) and (now() - '1 day'::interval) order by epochtime asc \",sep=\"\");
	segplot<-pg.spi.exec(query);
	segplot\$date_trunc<-as.Date(segplot\$date_trunc);
	###plot(segplot,col=\"red\",pch=20,type=\"p\",xlab=\"date\",ylab=\"% used\",main=paste(\"\",fsnam,\"\",sep=\"\"),ylim=c(0,100));
	plot(segplot,col=\"red\",pch=20,type=\"p\",xlab=\"date\",ylab=\"% used\",main=paste(\"\",fsnam,\"\",sep=\"\"),ylim=c(0,100),xlim=c(as.Date(gstart),as.Date(gend)),xaxt=\"n\");
	axis(1,xaxp=c(as.Date(gstart),as.Date(gend),arg3),at=seq(as.Date(gstart),as.Date(gend),by=1),labels=c(dlabels));
	lines(segplot,col=\"blue\",type=\"l\");
}

title(paste(\"Storage Array Data for \",arg1,\" \",sep=\"\"),sub = paste(\"Generated:\",date(),\" \",sep=\"\"),outer=TRUE,col.main=\"red\",font.sub=30,col.sub=\"blue\");

dev.off();

system(paste(\"chmod 644 \",ffilename,sep=\"\"));

par(mfrow=c(1,1));

\$\$ language plr;
" ;

$sth = $dbh->prepare($psqlcmd);

if ( $flags{'verbose'} )
{
	print "\nsth - after prepare\n";
	print Dumper $sth;
}

$sth->execute() || die $DBI::errstr;

if ( $flags{'verbose'} )
{
	print "\nsth - after execute\n";
	print Dumper $sth;
}

$sth->finish();
$dbh->disconnect();

##
## Now generate the graphics for the web site
##

##
## Now call the various functions with all appropriate data for each picture
##
foreach $site (@sites)
{
	foreach $tspan (@tspans)
	{
		$dest = "$site" . "_ba_storage";
		chomp($dest);

		@mps = `/usr/bin/psql -At -U postgres watcher -c "select distinct(fsname) from $dest where epochtime between (now() - '$tspan days'::interval) and (now() - '1 day'::interval) order by fsname asc"`;
		chomp(@mps);

		if ( $flags{'verbose'} )
		{
			print "\ntable name \n";
			print Dumper $dest;
			print "\nsite mnemonic \n";
			print Dumper $site;
			print "\nmount points to report\n";
			print Dumper \@mps;
		}

		$nplots = scalar (@mps);
		chomp($nplots);

		if ( $flags{'verbose'} )
		{
			print "\nnumber of mount points\n";
			print Dumper $nplots;
		}

		if ( $nplots <= 0 )
		{
			print "\n\nERROR: No storage data to plot for site $site time span $tspan. \n\n";
		}
		else
		{

			$msize = rxc();
			chomp($msize);

			if ( $flags{'verbose'} )
			{
				print "\nmatrix dimensions\n";
				print Dumper $msize;
			}

#
# Produce the plot for this site and tspan for all mount points
#
			print "\nPrinting Storage plots for $site and $tspan days \n";

			system("/usr/bin/psql -At -h localhost -U postgres watcher -c \"select storage_plot('$site', $tspan, $msize, $nplots)\"") == 0 or die "FATAL ERROR: Could not run function storage_plot \n";

		}
	}
}

##
## End main  -  Begin subroutines
##

#
# Calulate the size of the plot area in rows and columns
#
sub rxc()
{
	# square = 1.0, 3:4 = 1.333, 9:16 = 1.7777    row:column   ratio = column/row
	my $ratio = 1.0;

	my $col = ceil(($nplots ** ($ratio/2.0)));
	my $row = ceil(($nplots/$col));

	return "$row,$col";

}

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

