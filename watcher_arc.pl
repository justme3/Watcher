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
my @tspans = (30,60,90,180);

##
## variables
##
my @record = ();
my @records = ();
my $line = ();
my $zoom = ();
my $seg = ();
my @segs = ();
my @ssegs = ();
my @lsegs = ();
my @list = ();
my @solrips1 = ();
my @solrips2 = ();
my $region = ();
my @regions = ();
my %seen = ();
my $ip = ();
my $table = ();
my $dest = ();
my $destsrv = ();
my $index = ();
my $nindex = ();
my $timestamp = ();
my $tspan = ();
my $sth = ();
my $dbh = ();
my $psqlcmd = ();
my @locs = ();
my $loc1 = ();
my $loc2 = ();
my $site1 = ();
my $site2 = ();


##
## Get config information for which regions to draw pretty pictures for from table names in DB
##
	@regions = `/usr/bin/psql -At -U postgres watcher -c "select relname from pg_class where relname ~ '_arc_' order by relname" | awk -F\_ '{print \$1}'`;
	###@regions = qw(anz);
	chomp(@regions);

#
# Make the list of regions unique
#
	undef %seen;
   @regions = grep(!$seen{$_}++, @regions);

	if ( $flags{'verbose'} )
	{
		print "\nlist of regions\n";
		print Dumper \@regions;
	}

##
## Now generate the graphics for the web site
##

##
## Create all the functions we will need for graph creation
##
#
# plot all data for solr segments
#
$dbh = DBI->connect("dbi:Pg:dbname=watcher;host=localhost","postgres","") || die $DBI::errstr;

if ( $flags{'verbose'} )
{
	print "\nconnect status in dbh\n";
	print Dumper $dbh;
}

#                                                          arg1    arg2       arg3    arg4     arg5     arg6
#											 select arc_solr_seg_plot('$seg', '$region', $tspan, '$loc1', '$loc2', $zoom)
$psqlcmd = "create or replace function arc_solr_seg_plot(varchar, varchar, integer, varchar, varchar, integer) returns void as \$\$

filename<-paste(\"\",arg2,\"-\",arg1,\"-\",arg3,\"-\",arg6,\".png\",sep=\"\");
ffilename<-paste(\"\",\"var\",\"www\",\"html\",\"watcher\",\"archiving\",filename,sep=\"/\");
png(ffilename,width = 1200*arg6,height = 1000*arg6,units = \"px\",pointsize = 16);

gstart<-format((Sys.time() - ((3600*24)*arg3)), \"%Y-%m-%d\");
gend<-format(Sys.time(), \"%Y-%m-%d\");

ticks<-arg3;
if (arg3 == 30)  switch(arg6,ticks<-5,ticks<-10,ticks<-15,ticks<-15,ticks<-30);
if (arg3 == 60)  switch(arg6,ticks<-5,ticks<-12,ticks<-20,ticks<-20,ticks<-30);
if (arg3 == 90)  switch(arg6,ticks<-5,ticks<-12,ticks<-18,ticks<-22,ticks<-30);
if (arg3 == 180) switch(arg6,ticks<-5,ticks<-12,ticks<-20,ticks<-25,ticks<-30);

pertick<-as.integer(arg3/ticks);

dlabels<-c();
for ( x in (ticks:0))
{
   lbint<-c(format(Sys.time() - (((3600*24)*arg3)*(x/ticks)), \"%m-%d\"));
   dlabels<-c(c(dlabels), c(lbint));
}

par(mfrow=c(4,4),oma=c(5,0,3,0),cex.main=2);

# 1,1
query<<-paste(\"WITH dm as (SELECT date_trunc('day', ac1.epochtime), max(ac1.epochtime) epochtime, ac1.cid, (SELECT ingest24 FROM \",arg2,\"_arc_cid where \",arg2,\"_arc_cid.cid = ac1.cid and \",arg2,\"_arc_cid.epochtime=max(ac1.epochtime) and solrlabel ~ '\",arg1,\".solr.region-' ) as ingest24 from \",arg2,\"_arc_cid ac1 where epochtime between (now() - '\",arg3,\" days'::interval) and (now() - '6 hours'::interval) group by 1,3) SELECT dm.date_trunc::date, sum(dm.ingest24) from dm group by 1 order by 1\",sep=\"\");
segplot<<-pg.spi.exec(query);
segplot\$date_trunc<-as.Date(segplot\$date_trunc);
plot(segplot,col=\"red\",pch=20,type=\"p\",xlab=\"date\",ylab=\"ingest rate\",main=\"ingest/day\",xlim=c(as.Date(gstart),as.Date(gend)),xaxt=\"n\");
axis(1,xaxp=c(as.Date(gstart),as.Date(gend),pertick),at=seq(as.Date(gstart),as.Date(gend),by=pertick),labels=c(dlabels));
lines(segplot,col=\"blue\",type=\"l\");

#1,2
query<-paste(\"WITH dm as (SELECT date_trunc('day', ac1.epochtime), max(ac1.epochtime) epochtime, ac1.cid, (SELECT ndx24 FROM \",arg2,\"_arc_cid where \",arg2,\"_arc_cid.cid = ac1.cid and \",arg2,\"_arc_cid.epochtime=max(ac1.epochtime) and solrlabel ~ '\",arg1,\".solr.region-' ) as ndx24 from \",arg2,\"_arc_cid ac1 where epochtime between (now() - '\",arg3,\" days'::interval) and (now() - '6 hours'::interval) group by 1,3) SELECT dm.date_trunc::date, sum(dm.ndx24) from dm group by 1 order by 1\",sep=\"\");
segplot<-pg.spi.exec(query);
segplot\$date_trunc<-as.Date(segplot\$date_trunc);
plot(segplot,col=\"red\",pch=20,type=\"p\",xlab=\"date\",ylab=\"index rate\",main=\"index/day\",xlim=c(as.Date(gstart),as.Date(gend)),xaxt=\"n\");
axis(1,xaxp=c(as.Date(gstart),as.Date(gend),pertick),at=seq(as.Date(gstart),as.Date(gend),by=pertick),labels=c(dlabels));
lines(segplot,col=\"blue\",type=\"l\");

#1,3
query<-paste(\"WITH dm as (SELECT date_trunc('day', ac1.epochtime), max(ac1.epochtime) epochtime, ac1.cid, (SELECT solrndxbklg FROM \",arg2,\"_arc_cid where \",arg2,\"_arc_cid.cid = ac1.cid and \",arg2,\"_arc_cid.epochtime=max(ac1.epochtime) and solrlabel ~ '\",arg1,\".solr.region-' ) as solrndxbklg from \",arg2,\"_arc_cid ac1 where epochtime between (now() - '\",arg3,\" days'::interval) and (now() - '6 hours'::interval) group by 1,3) SELECT dm.date_trunc::date, sum(dm.solrndxbklg) from dm group by 1 order by 1\",sep=\"\");
segplot<-pg.spi.exec(query);
segplot\$date_trunc<-as.Date(segplot\$date_trunc);
plot(segplot,col=\"red\",pch=20,type=\"p\",xlab=\"date\",ylab=\"solr index backlog\",main=\"index backlog\",xlim=c(as.Date(gstart),as.Date(gend)),xaxt=\"n\");
axis(1,xaxp=c(as.Date(gstart),as.Date(gend),pertick),at=seq(as.Date(gstart),as.Date(gend),by=pertick),labels=c(dlabels));
lines(segplot,col=\"blue\",type=\"l\");

#1,4
query<-paste(\"WITH dm as (SELECT date_trunc('day', ac1.epochtime), max(ac1.epochtime) epochtime, ac1.cid, (SELECT mscnt FROM \",arg2,\"_arc_cid where \",arg2,\"_arc_cid.cid = ac1.cid and \",arg2,\"_arc_cid.epochtime=max(ac1.epochtime) and solrlabel ~ '\",arg1,\".solr.region-' ) as mscnt from \",arg2,\"_arc_cid ac1 where epochtime between (now() - '\",arg3,\" days'::interval) and (now() - '6 hours'::interval) group by 1,3) SELECT dm.date_trunc::date, sum(dm.mscnt) from dm group by 1 order by 1\",sep=\"\");
segplot<-pg.spi.exec(query);
segplot\$date_trunc<-as.Date(segplot\$date_trunc);
plot(segplot,col=\"red\",pch=20,type=\"p\",xlab=\"date\",ylab=\"Mail Sources\",main=\"number mail sources\",xlim=c(as.Date(gstart),as.Date(gend)),xaxt=\"n\");
axis(1,xaxp=c(as.Date(gstart),as.Date(gend),pertick),at=seq(as.Date(gstart),as.Date(gend),by=pertick),labels=c(dlabels));
lines(segplot,col=\"blue\",type=\"l\");

#2,1
query<-paste(\"select max(sum) from (WITH dm as (SELECT date_trunc('day', ac1.epochtime), max(ac1.epochtime) epochtime, ac1.cid, (SELECT ((coalesce(solrcnt[1],0)+coalesce(solrcnt[3],0)+coalesce(solrcnt[5],0)+coalesce(solrcnt[7],0)+coalesce(solrcnt[9],0))) FROM \",arg2,\"_arc_cid where \",arg2,\"_arc_cid.cid = ac1.cid and \",arg2,\"_arc_cid.epochtime=max(ac1.epochtime) and solrlabel ~ '\",arg1,\".solr.region-' ) as solrcnt from \",arg2,\"_arc_cid ac1 where epochtime between (now() - '\",arg3,\" days'::interval) and (now() - '6 hours'::interval) group by 1,3) SELECT dm.date_trunc::date, sum(dm.solrcnt) from dm group by 1 order by 1) as foo\",sep=\"\");
maxcnt<-pg.spi.exec(query);
index<-1;

while ( maxcnt > 1000 )
{
	index<-index*1000;
	maxcnt<-maxcnt/1000;
}

divisor<-index;

query<-paste(\"WITH dm as (SELECT date_trunc('day', ac1.epochtime), max(ac1.epochtime) epochtime, ac1.cid, (SELECT ((coalesce(solrcnt[1],0)+coalesce(solrcnt[3],0)+coalesce(solrcnt[5],0)+coalesce(solrcnt[7],0)+coalesce(solrcnt[9],0))/\",divisor,\") FROM \",arg2,\"_arc_cid where \",arg2,\"_arc_cid.cid = ac1.cid and \",arg2,\"_arc_cid.epochtime=max(ac1.epochtime) and solrlabel ~ '\",arg1,\".solr.region-' ) as solrcnt from \",arg2,\"_arc_cid ac1 where epochtime between (now() - '\",arg3,\" days'::interval) and (now() - '6 hours'::interval) group by 1,3) SELECT dm.date_trunc::date, sum(dm.solrcnt) from dm group by 1 order by 1\",sep=\"\");
segplot<-pg.spi.exec(query);
segplot\$date_trunc<-as.Date(segplot\$date_trunc);
plot(segplot,col=\"red\",pch=20,type=\"p\",xlab=\"date\",ylab=paste(\"solr count (\",divisor,\") - \",arg4,sep=\"\"),main=paste(\"\",arg4,\" solr cnt\",sep=\"\"),xlim=c(as.Date(gstart),as.Date(gend)),xaxt=\"n\");
axis(1,xaxp=c(as.Date(gstart),as.Date(gend),pertick),at=seq(as.Date(gstart),as.Date(gend),by=pertick),labels=c(dlabels));
lines(segplot,col=\"blue\",type=\"l\");

#2,2
query<-paste(\"select max(sum) from (WITH dm as (SELECT date_trunc('day', ac1.epochtime), max(ac1.epochtime) epochtime, ac1.cid, (SELECT ((coalesce(solrcnt[2],0)+coalesce(solrcnt[4],0)+coalesce(solrcnt[6],0)+coalesce(solrcnt[8],0)+coalesce(solrcnt[10],0))) FROM \",arg2,\"_arc_cid where \",arg2,\"_arc_cid.cid = ac1.cid and \",arg2,\"_arc_cid.epochtime=max(ac1.epochtime) and solrlabel ~ '\",arg1,\".solr.region-' ) as solrcnt from \",arg2,\"_arc_cid ac1 where epochtime between (now() - '\",arg3,\" days'::interval) and (now() - '6 hours'::interval) group by 1,3) SELECT dm.date_trunc::date, sum(dm.solrcnt) from dm group by 1 order by 1) as foo\",sep=\"\");
maxcnt<-pg.spi.exec(query);
index<-1;

while ( maxcnt > 1000 )
{
	index<-index*1000;
	maxcnt<-maxcnt/1000;
}

query<-paste(\"WITH dm as (SELECT date_trunc('day', ac1.epochtime), max(ac1.epochtime) epochtime, ac1.cid, (SELECT ((coalesce(solrcnt[2],0)+coalesce(solrcnt[4],0)+coalesce(solrcnt[6],0)+coalesce(solrcnt[8],0)+coalesce(solrcnt[10],0))/\",divisor,\") FROM \",arg2,\"_arc_cid where \",arg2,\"_arc_cid.cid = ac1.cid and \",arg2,\"_arc_cid.epochtime=max(ac1.epochtime) and solrlabel ~ '\",arg1,\".solr.region-' ) as solrcnt from \",arg2,\"_arc_cid ac1 where epochtime between (now() - '\",arg3,\" days'::interval) and (now() - '6 hours'::interval) group by 1,3) SELECT dm.date_trunc::date, sum(dm.solrcnt) from dm group by 1 order by 1\",sep=\"\");
segplot<-pg.spi.exec(query);
segplot\$date_trunc<-as.Date(segplot\$date_trunc);
plot(segplot,col=\"red\",pch=20,type=\"p\",xlab=\"date\",ylab=paste(\"solr count (\",divisor,\") - \",arg5,sep=\"\"),main=paste(\"\",arg5,\" solr cnt\",sep=\"\"),xlim=c(as.Date(gstart),as.Date(gend)),xaxt=\"n\");
axis(1,xaxp=c(as.Date(gstart),as.Date(gend),pertick),at=seq(as.Date(gstart),as.Date(gend),by=pertick),labels=c(dlabels));
lines(segplot,col=\"blue\",type=\"l\");

#2,3
query<-paste(\"WITH dm as (SELECT date_trunc('day', ac1.epochtime), max(ac1.epochtime) epochtime, ac1.cid, (SELECT coalesce(solrej[1],0)+coalesce(solrej[3],0)+coalesce(solrej[5],0)+coalesce(solrej[7],0)+coalesce(solrej[9],0) FROM \",arg2,\"_arc_cid where \",arg2,\"_arc_cid.cid = ac1.cid and \",arg2,\"_arc_cid.epochtime=max(ac1.epochtime) and solrlabel ~ '\",arg1,\".solr.region-' ) as solrcnt from \",arg2,\"_arc_cid ac1 where epochtime between (now() - '\",arg3,\" days'::interval) and (now() - '6 hours'::interval) group by 1,3) SELECT dm.date_trunc::date, sum(dm.solrcnt) from dm group by 1 order by 1\",sep=\"\");
segplot<-pg.spi.exec(query);
segplot\$date_trunc<-as.Date(segplot\$date_trunc);
plot(segplot,col=\"red\",pch=20,type=\"p\",xlab=\"date\",ylab=paste(\"solr rejects - \",arg4,sep=\"\"),main=paste(\"\",arg4,\" solr rejects\",sep=\"\"),xlim=c(as.Date(gstart),as.Date(gend)),xaxt=\"n\");
axis(1,xaxp=c(as.Date(gstart),as.Date(gend),pertick),at=seq(as.Date(gstart),as.Date(gend),by=pertick),labels=c(dlabels));
lines(segplot,col=\"blue\",type=\"l\");

#2,4
query<-paste(\"WITH dm as (SELECT date_trunc('day', ac1.epochtime), max(ac1.epochtime) epochtime, ac1.cid, (SELECT coalesce(solrej[2],0)+coalesce(solrej[4],0)+coalesce(solrej[6],0)+coalesce(solrej[8],0)+coalesce(solrej[10],0) FROM \",arg2,\"_arc_cid where \",arg2,\"_arc_cid.cid = ac1.cid and \",arg2,\"_arc_cid.epochtime=max(ac1.epochtime) and solrlabel ~ '\",arg1,\".solr.region-' ) as solrcnt from \",arg2,\"_arc_cid ac1 where epochtime between (now() - '\",arg3,\" days'::interval) and (now() - '6 hours'::interval) group by 1,3) SELECT dm.date_trunc::date, sum(dm.solrcnt) from dm group by 1 order by 1\",sep=\"\");
segplot<-pg.spi.exec(query);
segplot\$date_trunc<-as.Date(segplot\$date_trunc);
plot(segplot,col=\"red\",pch=20,type=\"p\",xlab=\"date\",ylab=paste(\"solr rejects - \",arg5,sep=\"\"),main=paste(\"\",arg5,\" solr rejects\",sep=\"\"),xlim=c(as.Date(gstart),as.Date(gend)),xaxt=\"n\");
axis(1,xaxp=c(as.Date(gstart),as.Date(gend),pertick),at=seq(as.Date(gstart),as.Date(gend),by=pertick),labels=c(dlabels));
lines(segplot,col=\"blue\",type=\"l\");

#3,1
query<-paste(\"WITH dm as (SELECT date_trunc('day', ac1.epochtime), max(ac1.epochtime) epochtime, ac1.cid, (SELECT coalesce(sizemb[1],0)+coalesce(sizemb[3],0)+coalesce(sizemb[5],0)+coalesce(sizemb[7],0)+coalesce(sizemb[9],0) FROM \",arg2,\"_arc_cid where \",arg2,\"_arc_cid.cid = ac1.cid and \",arg2,\"_arc_cid.epochtime=max(ac1.epochtime) and solrlabel ~ '\",arg1,\".solr.region-' ) as solrcnt from \",arg2,\"_arc_cid ac1 where epochtime between (now() - '\",arg3,\" days'::interval) and (now() - '6 hours'::interval) group by 1,3) SELECT dm.date_trunc::date, sum(dm.solrcnt) from dm group by 1 order by 1\",sep=\"\");

###pg.thrownotice(query);

segplot<-pg.spi.exec(query);
segplot\$date_trunc<-as.Date(segplot\$date_trunc);
plot(segplot,col=\"red\",pch=20,type=\"p\",xlab=\"date\",ylab=paste(\"Size on Disk (MB) - \",arg4,sep=\"\"),main=paste(\"\",arg4,\" disk size\",sep=\"\"),xlim=c(as.Date(gstart),as.Date(gend)),xaxt=\"n\");
axis(1,xaxp=c(as.Date(gstart),as.Date(gend),pertick),at=seq(as.Date(gstart),as.Date(gend),by=pertick),labels=c(dlabels));
lines(segplot,col=\"blue\",type=\"l\");

#3,2
query<-paste(\"WITH dm as (SELECT date_trunc('day', ac1.epochtime), max(ac1.epochtime) epochtime, ac1.cid, (SELECT coalesce(sizemb[2],0)+coalesce(sizemb[4],0)+coalesce(sizemb[6],0)+coalesce(sizemb[8],0)+coalesce(sizemb[10],0) FROM \",arg2,\"_arc_cid where \",arg2,\"_arc_cid.cid = ac1.cid and \",arg2,\"_arc_cid.epochtime=max(ac1.epochtime) and solrlabel ~ '\",arg1,\".solr.region-' ) as solrcnt from \",arg2,\"_arc_cid ac1 where epochtime between (now() - '\",arg3,\" days'::interval) and (now() - '6 hours'::interval) group by 1,3) SELECT dm.date_trunc::date, sum(dm.solrcnt) from dm group by 1 order by 1\",sep=\"\");
segplot<-pg.spi.exec(query);
segplot\$date_trunc<-as.Date(segplot\$date_trunc);
plot(segplot,col=\"red\",pch=20,type=\"p\",xlab=\"date\",ylab=paste(\"Size on Disk (MB) - \",arg5,sep=\"\"),main=paste(\"\",arg5,\" disk size\",sep=\"\"),xlim=c(as.Date(gstart),as.Date(gend)),xaxt=\"n\");
axis(1,xaxp=c(as.Date(gstart),as.Date(gend),pertick),at=seq(as.Date(gstart),as.Date(gend),by=pertick),labels=c(dlabels));
lines(segplot,col=\"blue\",type=\"l\");

#3,3
query<-paste(\"WITH dm as (SELECT date_trunc('day', ac1.epochtime), max(ac1.epochtime) epochtime, ac1.cid, (SELECT coalesce(severe[1],0)+coalesce(severe[3],0)+coalesce(severe[5],0)+coalesce(severe[7],0)+coalesce(severe[9],0) FROM \",arg2,\"_arc_cid where \",arg2,\"_arc_cid.cid = ac1.cid and \",arg2,\"_arc_cid.epochtime=max(ac1.epochtime) and solrlabel ~ '\",arg1,\".solr.region-' ) as solrcnt from \",arg2,\"_arc_cid ac1 where epochtime between (now() - '\",arg3,\" days'::interval) and (now() - '6 hours'::interval) group by 1,3) SELECT dm.date_trunc::date, sum(dm.solrcnt) from dm group by 1 order by 1\",sep=\"\");
segplot<-pg.spi.exec(query);
segplot\$date_trunc<-as.Date(segplot\$date_trunc);
plot(segplot,col=\"red\",pch=20,type=\"p\",xlab=\"date\",ylab=paste(\"severe errors - \",arg4,sep=\"\"),main=paste(\"\",arg4,\" severe errors\",sep=\"\"),xlim=c(as.Date(gstart),as.Date(gend)),xaxt=\"n\");
axis(1,xaxp=c(as.Date(gstart),as.Date(gend),pertick),at=seq(as.Date(gstart),as.Date(gend),by=pertick),labels=c(dlabels));
lines(segplot,col=\"blue\",type=\"l\");

#3,4
query<-paste(\"WITH dm as (SELECT date_trunc('day', ac1.epochtime), max(ac1.epochtime) epochtime, ac1.cid, (SELECT coalesce(severe[2],0)+coalesce(severe[4],0)+coalesce(severe[6],0)+coalesce(severe[8],0)+coalesce(severe[10],0) FROM \",arg2,\"_arc_cid where \",arg2,\"_arc_cid.cid = ac1.cid and \",arg2,\"_arc_cid.epochtime=max(ac1.epochtime) and solrlabel ~ '\",arg1,\".solr.region-' ) as solrcnt from \",arg2,\"_arc_cid ac1 where epochtime between (now() - '\",arg3,\" days'::interval) and (now() - '6 hours'::interval) group by 1,3) SELECT dm.date_trunc::date, sum(dm.solrcnt) from dm group by 1 order by 1\",sep=\"\");
segplot<-pg.spi.exec(query);
segplot\$date_trunc<-as.Date(segplot\$date_trunc);
plot(segplot,col=\"red\",pch=20,type=\"p\",xlab=\"date\",ylab=paste(\"severe errors - \",arg5,sep=\"\"),main=paste(\"\",arg5,\" severe errors\",sep=\"\"),xlim=c(as.Date(gstart),as.Date(gend)),xaxt=\"n\");
axis(1,xaxp=c(as.Date(gstart),as.Date(gend),pertick),at=seq(as.Date(gstart),as.Date(gend),by=pertick),labels=c(dlabels));
lines(segplot,col=\"blue\",type=\"l\");

#4,1
query<-paste(\"WITH dm as (SELECT date_trunc('day', ac1.epochtime), max(ac1.epochtime) epochtime, ac1.cid, (SELECT count(distinct cid) FROM \",arg2,\"_arc_cid where \",arg2,\"_arc_cid.cid = ac1.cid and \",arg2,\"_arc_cid.epochtime=max(ac1.epochtime) and solrlabel ~ '\",arg1,\".solr.region-' ) as numcid from \",arg2,\"_arc_cid ac1 where epochtime between (now() - '\",arg3,\" days'::interval) and (now() - '6 hours'::interval) group by 1,3) SELECT dm.date_trunc::date, sum(dm.numcid) from dm group by 1 order by 1\",sep=\"\");
segplot<-pg.spi.exec(query);
segplot\$date_trunc<-as.Date(segplot\$date_trunc);
plot(segplot,col=\"red\",pch=20,type=\"p\",xlab=\"date\",ylab=\"number of cids\",main=\"Number of CIDs\",xlim=c(as.Date(gstart),as.Date(gend)),xaxt=\"n\");
axis(1,xaxp=c(as.Date(gstart),as.Date(gend),pertick),at=seq(as.Date(gstart),as.Date(gend),by=pertick),labels=c(dlabels));
lines(segplot,col=\"blue\",type=\"l\");

#4,2
query<-paste(\"WITH dm as (SELECT date_trunc('day', ac1.epochtime), max(ac1.epochtime) epochtime, ac1.cid, (SELECT usrcnt FROM \",arg2,\"_arc_cid where \",arg2,\"_arc_cid.cid = ac1.cid and \",arg2,\"_arc_cid.epochtime=max(ac1.epochtime) and solrlabel ~ '\",arg1,\".solr.region-' ) as usrcnt from \",arg2,\"_arc_cid ac1 where epochtime between (now() - '\",arg3,\" days'::interval) and (now() - '6 hours'::interval) group by 1,3) SELECT dm.date_trunc::date, sum(dm.usrcnt) from dm group by 1 order by 1\",sep=\"\");
segplot<-pg.spi.exec(query);
segplot\$date_trunc<-as.Date(segplot\$date_trunc);
plot(segplot,col=\"red\",pch=20,type=\"p\",xlab=\"date\",ylab=\"number of users\",main=\"Number of users\",xlim=c(as.Date(gstart),as.Date(gend)),xaxt=\"n\");
axis(1,xaxp=c(as.Date(gstart),as.Date(gend),pertick),at=seq(as.Date(gstart),as.Date(gend),by=pertick),labels=c(dlabels));
lines(segplot,col=\"blue\",type=\"l\");

#4,3
query<-paste(\"WITH dm as (SELECT date_trunc('day', ac1.epochtime), max(ac1.epochtime) epochtime, ac1.cid, (SELECT masrej FROM \",arg2,\"_arc_cid where \",arg2,\"_arc_cid.cid = ac1.cid and \",arg2,\"_arc_cid.epochtime=max(ac1.epochtime) and solrlabel ~ '\",arg1,\".solr.region-' ) as masrej from \",arg2,\"_arc_cid ac1 where epochtime between (now() - '\",arg3,\" days'::interval) and (now() - '6 hours'::interval) group by 1,3) SELECT dm.date_trunc::date, sum(dm.masrej) from dm group by 1 order by 1\",sep=\"\");
segplot<-pg.spi.exec(query);
segplot\$date_trunc<-as.Date(segplot\$date_trunc);
plot(segplot,col=\"red\",pch=20,type=\"p\",xlab=\"date\",ylab=\"mas rejects\",main=\"MAS Rejects\",xlim=c(as.Date(gstart),as.Date(gend)),xaxt=\"n\");
axis(1,xaxp=c(as.Date(gstart),as.Date(gend),pertick),at=seq(as.Date(gstart),as.Date(gend),by=pertick),labels=c(dlabels));
lines(segplot,col=\"blue\",type=\"l\");

#4,4
#

title(paste(\"Segment \",arg1,\" Detailed Data - \",arg2,\" \",sep=\"\"), sub = paste(\"Generated:\",date(),\" \",sep=\"\"),outer=TRUE, col.main= \"red\", font.sub = 30, col.sub = \"blue\");

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

#
# define graphs for the summary of all solr segments for each region
#
$dbh = DBI->connect("dbi:Pg:dbname=watcher;host=localhost","postgres","") || die $DBI::errstr;


{
	print "\nconnect status in dbh\n";
	print Dumper $dbh;
}

#										  select arc_solr_plot('', '$region', $tspan, '$loc1', '$loc2', $zoom)
$psqlcmd = "create or replace function arc_solr_plot(varchar, varchar, integer, varchar, varchar, integer) returns void as \$\$

filename<-paste(\"\",arg2,\"-sum-solr-segments-\",arg3,\"-\",arg6,\".png\",sep=\"\");
ffilename<-paste(\"\",\"var\",\"www\",\"html\",\"watcher\",\"archiving\",filename,sep=\"/\");
png(ffilename,width = 1200*arg6,height = 1000*arg6,units = \"px\",pointsize = 16);

gstart<-format((Sys.time() - ((3600*24)*arg3)), \"%Y-%m-%d\");
gend<-format(Sys.time(), \"%Y-%m-%d\");

ticks<-arg3;
if (arg3 == 30)  switch(arg6,ticks<-5,ticks<-10,ticks<-15,ticks<-15,ticks<-30);
if (arg3 == 60)  switch(arg6,ticks<-5,ticks<-12,ticks<-20,ticks<-20,ticks<-30);
if (arg3 == 90)  switch(arg6,ticks<-5,ticks<-12,ticks<-18,ticks<-22,ticks<-30);
if (arg3 == 180) switch(arg6,ticks<-5,ticks<-12,ticks<-20,ticks<-25,ticks<-30);

pertick<-as.integer(arg3/ticks);

###pg.thrownotice(ticks);
###pg.thrownotice(pertick);

dlabels<-c();
for ( x in (ticks:0)) 
{
	lbint<-c(format(Sys.time() - (((3600*24)*arg3)*(x/ticks)), \"%m-%d\"));
	dlabels<-c(c(dlabels), c(lbint));
}

par(mfrow=c(4,4),oma=c(5,0,3,0),cex.main=2);

# 1,1
query<<-paste(\"WITH dm as (SELECT date_trunc('day', ac1.epochtime), max(ac1.epochtime) epochtime, ac1.cid, (SELECT ingest24 FROM \",arg2,\"_arc_cid where \",arg2,\"_arc_cid.cid = ac1.cid and \",arg2,\"_arc_cid.epochtime=max(ac1.epochtime) ) as ingest24 from \",arg2,\"_arc_cid ac1 where epochtime between (now() - '\",arg3,\" days'::interval) and (now() - '6 hours'::interval) group by 1,3) SELECT dm.date_trunc::date, sum(dm.ingest24) from dm group by 1 order by 1\",sep=\"\");
segplot<<-pg.spi.exec(query);
segplot\$date_trunc<-as.Date(segplot\$date_trunc);
###plot(segplot,col=\"red\",pch=20,type=\"p\",xlab=\"date\",ylab=\"ingest rate\",main=\"ingest/day\");
plot(segplot,col=\"red\",pch=20,type=\"p\",xlab=\"date\",ylab=\"ingest rate\",main=\"ingest/day\",xlim=c(as.Date(gstart),as.Date(gend)),xaxt=\"n\");
axis(1,xaxp=c(as.Date(gstart),as.Date(gend),pertick),at=seq(as.Date(gstart),as.Date(gend),by=pertick),labels=c(dlabels));
lines(segplot,col=\"blue\",type=\"l\");

#1,2
query<-paste(\"WITH dm as (SELECT date_trunc('day', ac1.epochtime), max(ac1.epochtime) epochtime, ac1.cid, (SELECT ndx24 FROM \",arg2,\"_arc_cid where \",arg2,\"_arc_cid.cid = ac1.cid and \",arg2,\"_arc_cid.epochtime=max(ac1.epochtime) ) as ndx24 from \",arg2,\"_arc_cid ac1 where epochtime between (now() - '\",arg3,\" days'::interval) and (now() - '6 hours'::interval) group by 1,3) SELECT dm.date_trunc::date, sum(dm.ndx24) from dm group by 1 order by 1\",sep=\"\");
segplot<-pg.spi.exec(query);
segplot\$date_trunc<-as.Date(segplot\$date_trunc);
plot(segplot,col=\"red\",pch=20,type=\"p\",xlab=\"date\",ylab=\"index rate\",main=\"index/day\",xlim=c(as.Date(gstart),as.Date(gend)),xaxt=\"n\");
axis(1,xaxp=c(as.Date(gstart),as.Date(gend),pertick),at=seq(as.Date(gstart),as.Date(gend),by=pertick),labels=c(dlabels));
lines(segplot,col=\"blue\",type=\"l\");

#1,3
query<-paste(\"WITH dm as (SELECT date_trunc('day', ac1.epochtime), max(ac1.epochtime) epochtime, ac1.cid, (SELECT solrndxbklg FROM \",arg2,\"_arc_cid where \",arg2,\"_arc_cid.cid = ac1.cid and \",arg2,\"_arc_cid.epochtime=max(ac1.epochtime) ) as solrndxbklg from \",arg2,\"_arc_cid ac1 where epochtime between (now() - '\",arg3,\" days'::interval) and (now() - '6 hours'::interval) group by 1,3) SELECT dm.date_trunc::date, sum(dm.solrndxbklg) from dm group by 1 order by 1\",sep=\"\");
segplot<-pg.spi.exec(query);
segplot\$date_trunc<-as.Date(segplot\$date_trunc);
plot(segplot,col=\"red\",pch=20,type=\"p\",xlab=\"date\",ylab=\"solr index backlog\",main=\"index backlog\",xlim=c(as.Date(gstart),as.Date(gend)),xaxt=\"n\");
axis(1,xaxp=c(as.Date(gstart),as.Date(gend),pertick),at=seq(as.Date(gstart),as.Date(gend),by=pertick),labels=c(dlabels));
lines(segplot,col=\"blue\",type=\"l\");

#1,4
query<-paste(\"WITH dm as (SELECT date_trunc('day', ac1.epochtime), max(ac1.epochtime) epochtime, ac1.cid, (SELECT mscnt FROM \",arg2,\"_arc_cid where \",arg2,\"_arc_cid.cid = ac1.cid and \",arg2,\"_arc_cid.epochtime=max(ac1.epochtime) ) as mscnt from \",arg2,\"_arc_cid ac1 where epochtime between (now() - '\",arg3,\" days'::interval) and (now() - '6 hours'::interval) group by 1,3) SELECT dm.date_trunc::date, sum(dm.mscnt) from dm group by 1 order by 1\",sep=\"\");
segplot<-pg.spi.exec(query);
segplot\$date_trunc<-as.Date(segplot\$date_trunc);
plot(segplot,col=\"red\",pch=20,type=\"p\",xlab=\"date\",ylab=\"Mail Sources\",main=\"number mail sources\",xlim=c(as.Date(gstart),as.Date(gend)),xaxt=\"n\");
axis(1,xaxp=c(as.Date(gstart),as.Date(gend),pertick),at=seq(as.Date(gstart),as.Date(gend),by=pertick),labels=c(dlabels));
lines(segplot,col=\"blue\",type=\"l\");

#2,1
query<-paste(\"select max(sum) from (WITH dm as (SELECT date_trunc('day', ac1.epochtime), max(ac1.epochtime) epochtime, ac1.cid, (SELECT ((coalesce(solrcnt[1],0)+coalesce(solrcnt[3],0)+coalesce(solrcnt[5],0)+coalesce(solrcnt[7],0)+coalesce(solrcnt[9],0))) FROM \",arg2,\"_arc_cid where \",arg2,\"_arc_cid.cid = ac1.cid and \",arg2,\"_arc_cid.epochtime=max(ac1.epochtime) ) as solrcnt from \",arg2,\"_arc_cid ac1 where epochtime between (now() - '\",arg3,\" days'::interval) and (now() - '6 hours'::interval) group by 1,3) SELECT dm.date_trunc::date, sum(dm.solrcnt) from dm group by 1 order by 1) as foo\",sep=\"\");
maxcnt<-pg.spi.exec(query);
index<-1;

while ( maxcnt > 1000 )
{
	index<-index*1000;
	maxcnt<-maxcnt/1000;
}

divisor<-index;

query<-paste(\"WITH dm as (SELECT date_trunc('day', ac1.epochtime), max(ac1.epochtime) epochtime, ac1.cid, (SELECT ((coalesce(solrcnt[1],0)+coalesce(solrcnt[3],0)+coalesce(solrcnt[5],0)+coalesce(solrcnt[7],0)+coalesce(solrcnt[9],0))/\",divisor,\") FROM \",arg2,\"_arc_cid where \",arg2,\"_arc_cid.cid = ac1.cid and \",arg2,\"_arc_cid.epochtime=max(ac1.epochtime) ) as solrcnt from \",arg2,\"_arc_cid ac1 where epochtime between (now() - '\",arg3,\" days'::interval) and (now() - '6 hours'::interval) group by 1,3) SELECT dm.date_trunc::date, sum(dm.solrcnt) from dm group by 1 order by 1\",sep=\"\");
segplot<-pg.spi.exec(query);
segplot\$date_trunc<-as.Date(segplot\$date_trunc);
plot(segplot,col=\"red\",pch=20,type=\"p\",xlab=\"date\",ylab=paste(\"solr count (\",divisor,\") - \",arg4,sep=\"\"),main=paste(\"\",arg4,\" solr cnt\",sep=\"\"),xlim=c(as.Date(gstart),as.Date(gend)),xaxt=\"n\");
axis(1,xaxp=c(as.Date(gstart),as.Date(gend),pertick),at=seq(as.Date(gstart),as.Date(gend),by=pertick),labels=c(dlabels));
lines(segplot,col=\"blue\",type=\"l\");

#2,2
query<-paste(\"select max(sum) from (WITH dm as (SELECT date_trunc('day', ac1.epochtime), max(ac1.epochtime) epochtime, ac1.cid, (SELECT ((coalesce(solrcnt[2],0)+coalesce(solrcnt[4],0)+coalesce(solrcnt[6],0)+coalesce(solrcnt[8],0)+coalesce(solrcnt[10],0))) FROM \",arg2,\"_arc_cid where \",arg2,\"_arc_cid.cid = ac1.cid and \",arg2,\"_arc_cid.epochtime=max(ac1.epochtime) ) as solrcnt from \",arg2,\"_arc_cid ac1 where epochtime between (now() - '\",arg3,\" days'::interval) and (now() - '6 hours'::interval) group by 1,3) SELECT dm.date_trunc::date, sum(dm.solrcnt) from dm group by 1 order by 1) as foo\",sep=\"\");
maxcnt<-pg.spi.exec(query);
index<-1;

while ( maxcnt > 1000 )
{
	index<-index*1000;
	maxcnt<-maxcnt/1000;
}

divisor<-index;

query<-paste(\"WITH dm as (SELECT date_trunc('day', ac1.epochtime), max(ac1.epochtime) epochtime, ac1.cid, (SELECT ((coalesce(solrcnt[2],0)+coalesce(solrcnt[4],0)+coalesce(solrcnt[6],0)+coalesce(solrcnt[8],0)+coalesce(solrcnt[10],0))/\",divisor,\") FROM \",arg2,\"_arc_cid where \",arg2,\"_arc_cid.cid = ac1.cid and \",arg2,\"_arc_cid.epochtime=max(ac1.epochtime) ) as solrcnt from \",arg2,\"_arc_cid ac1 where epochtime between (now() - '\",arg3,\" days'::interval) and (now() - '6 hours'::interval) group by 1,3) SELECT dm.date_trunc::date, sum(dm.solrcnt) from dm group by 1 order by 1\",sep=\"\");
segplot<-pg.spi.exec(query);
segplot\$date_trunc<-as.Date(segplot\$date_trunc);
plot(segplot,col=\"red\",pch=20,type=\"p\",xlab=\"date\",ylab=paste(\"solr count (\",divisor,\") - \",arg5,sep=\"\"),main=paste(\"\",arg5,\" solr cnt\",sep=\"\"),xlim=c(as.Date(gstart),as.Date(gend)),xaxt=\"n\");
axis(1,xaxp=c(as.Date(gstart),as.Date(gend),pertick),at=seq(as.Date(gstart),as.Date(gend),by=pertick),labels=c(dlabels));
lines(segplot,col=\"blue\",type=\"l\");

#2,3
query<-paste(\"WITH dm as (SELECT date_trunc('day', ac1.epochtime), max(ac1.epochtime) epochtime, ac1.cid, (SELECT coalesce(solrej[1],0)+coalesce(solrej[3],0)+coalesce(solrej[5],0)+coalesce(solrej[7],0)+coalesce(solrej[9],0) FROM \",arg2,\"_arc_cid where \",arg2,\"_arc_cid.cid = ac1.cid and \",arg2,\"_arc_cid.epochtime=max(ac1.epochtime) ) as solrej from \",arg2,\"_arc_cid ac1 where epochtime between (now() - '\",arg3,\" days'::interval) and (now() - '6 hours'::interval) group by 1,3) SELECT dm.date_trunc::date, sum(dm.solrej) from dm group by 1 order by 1\",sep=\"\");
segplot<-pg.spi.exec(query);
segplot\$date_trunc<-as.Date(segplot\$date_trunc);
plot(segplot,col=\"red\",pch=20,type=\"p\",xlab=\"date\",ylab=paste(\"solr rejects - \",arg4,sep=\"\"),main=paste(\"\",arg4,\" solr rejects\",sep=\"\"),xlim=c(as.Date(gstart),as.Date(gend)),xaxt=\"n\");
axis(1,xaxp=c(as.Date(gstart),as.Date(gend),pertick),at=seq(as.Date(gstart),as.Date(gend),by=pertick),labels=c(dlabels));
lines(segplot,col=\"blue\",type=\"l\");

#2,4
query<-paste(\"WITH dm as (SELECT date_trunc('day', ac1.epochtime), max(ac1.epochtime) epochtime, ac1.cid, (SELECT coalesce(solrej[2],0)+coalesce(solrej[4],0)+coalesce(solrej[6],0)+coalesce(solrej[8],0)+coalesce(solrej[10],0) FROM \",arg2,\"_arc_cid where \",arg2,\"_arc_cid.cid = ac1.cid and \",arg2,\"_arc_cid.epochtime=max(ac1.epochtime) ) as solrej from \",arg2,\"_arc_cid ac1 where epochtime between (now() - '\",arg3,\" days'::interval) and (now() - '6 hours'::interval) group by 1,3) SELECT dm.date_trunc::date, sum(dm.solrej) from dm group by 1 order by 1\",sep=\"\");
segplot<-pg.spi.exec(query);
segplot\$date_trunc<-as.Date(segplot\$date_trunc);
plot(segplot,col=\"red\",pch=20,type=\"p\",xlab=\"date\",ylab=paste(\"solr rejects - \",arg5,sep=\"\"),main=paste(\"\",arg5,\" solr rejects\",sep=\"\"),xlim=c(as.Date(gstart),as.Date(gend)),xaxt=\"n\");
axis(1,xaxp=c(as.Date(gstart),as.Date(gend),pertick),at=seq(as.Date(gstart),as.Date(gend),by=pertick),labels=c(dlabels));
lines(segplot,col=\"blue\",type=\"l\");

#3,1
query<-paste(\"WITH dm as (SELECT date_trunc('day', ac1.epochtime), max(ac1.epochtime) epochtime, ac1.cid, (SELECT coalesce(sizemb[1],0)+coalesce(sizemb[3],0)+coalesce(sizemb[5],0)+coalesce(sizemb[7],0)+coalesce(sizemb[9],0) FROM \",arg2,\"_arc_cid where \",arg2,\"_arc_cid.cid = ac1.cid and \",arg2,\"_arc_cid.epochtime=max(ac1.epochtime) ) as sizemb from \",arg2,\"_arc_cid ac1 where epochtime between (now() - '\",arg3,\" days'::interval) and (now() - '6 hours'::interval) group by 1,3) SELECT dm.date_trunc::date, sum(dm.sizemb) from dm group by 1 order by 1\",sep=\"\");
segplot<-pg.spi.exec(query);
segplot\$date_trunc<-as.Date(segplot\$date_trunc);
plot(segplot,col=\"red\",pch=20,type=\"p\",xlab=\"date\",ylab=paste(\"Size on Disk (MB) - \",arg4,sep=\"\"),main=paste(\"\",arg4,\" disk size\",sep=\"\"),xlim=c(as.Date(gstart),as.Date(gend)),xaxt=\"n\");
axis(1,xaxp=c(as.Date(gstart),as.Date(gend),pertick),at=seq(as.Date(gstart),as.Date(gend),by=pertick),labels=c(dlabels));
lines(segplot,col=\"blue\",type=\"l\");

#3,2
query<-paste(\"WITH dm as (SELECT date_trunc('day', ac1.epochtime), max(ac1.epochtime) epochtime, ac1.cid, (SELECT coalesce(sizemb[2],0)+coalesce(sizemb[4],0)+coalesce(sizemb[6],0)+coalesce(sizemb[8],0)+coalesce(sizemb[10],0) FROM \",arg2,\"_arc_cid where \",arg2,\"_arc_cid.cid = ac1.cid and \",arg2,\"_arc_cid.epochtime=max(ac1.epochtime) ) as sizemb from \",arg2,\"_arc_cid ac1 where epochtime between (now() - '\",arg3,\" days'::interval) and (now() - '6 hours'::interval) group by 1,3) SELECT dm.date_trunc::date, sum(dm.sizemb) from dm group by 1 order by 1\",sep=\"\");
segplot<-pg.spi.exec(query);
segplot\$date_trunc<-as.Date(segplot\$date_trunc);
plot(segplot,col=\"red\",pch=20,type=\"p\",xlab=\"date\",ylab=paste(\"Size on Disk (MB) - \",arg5,sep=\"\"),main=paste(\"\",arg5,\" disk size\",sep=\"\"),xlim=c(as.Date(gstart),as.Date(gend)),xaxt=\"n\");
axis(1,xaxp=c(as.Date(gstart),as.Date(gend),pertick),at=seq(as.Date(gstart),as.Date(gend),by=pertick),labels=c(dlabels));
lines(segplot,col=\"blue\",type=\"l\");

#3,3
query<-paste(\"WITH dm as (SELECT date_trunc('day', ac1.epochtime), max(ac1.epochtime) epochtime, ac1.cid, (SELECT coalesce(severe[1],0)+coalesce(severe[3],0)+coalesce(severe[5],0)+coalesce(severe[7],0)+coalesce(severe[9],0) FROM \",arg2,\"_arc_cid where \",arg2,\"_arc_cid.cid = ac1.cid and \",arg2,\"_arc_cid.epochtime=max(ac1.epochtime) ) as severe from \",arg2,\"_arc_cid ac1 where epochtime between (now() - '\",arg3,\" days'::interval) and (now() - '6 hours'::interval) group by 1,3) SELECT dm.date_trunc::date, sum(dm.severe) from dm group by 1 order by 1\",sep=\"\");
segplot<-pg.spi.exec(query);
segplot\$date_trunc<-as.Date(segplot\$date_trunc);
plot(segplot,col=\"red\",pch=20,type=\"p\",xlab=\"date\",ylab=paste(\"severe errors - \",arg4,sep=\"\"),main=paste(\"\",arg4,\" severe errors\",sep=\"\"),xlim=c(as.Date(gstart),as.Date(gend)),xaxt=\"n\");
axis(1,xaxp=c(as.Date(gstart),as.Date(gend),pertick),at=seq(as.Date(gstart),as.Date(gend),by=pertick),labels=c(dlabels));
lines(segplot,col=\"blue\",type=\"l\");

#3,4
query<-paste(\"WITH dm as (SELECT date_trunc('day', ac1.epochtime), max(ac1.epochtime) epochtime, ac1.cid, (SELECT coalesce(severe[2],0)+coalesce(severe[4],0)+coalesce(severe[6],0)+coalesce(severe[8],0)+coalesce(severe[10],0) FROM \",arg2,\"_arc_cid where \",arg2,\"_arc_cid.cid = ac1.cid and \",arg2,\"_arc_cid.epochtime=max(ac1.epochtime) ) as severe from \",arg2,\"_arc_cid ac1 where epochtime between (now() - '\",arg3,\" days'::interval) and (now() - '6 hours'::interval) group by 1,3) SELECT dm.date_trunc::date, sum(dm.severe) from dm group by 1 order by 1\",sep=\"\");
segplot<-pg.spi.exec(query);
segplot\$date_trunc<-as.Date(segplot\$date_trunc);
plot(segplot,col=\"red\",pch=20,type=\"p\",xlab=\"date\",ylab=paste(\"severe errors - \",arg5,sep=\"\"),main=paste(\"\",arg5,\" severe errors\",sep=\"\"),xlim=c(as.Date(gstart),as.Date(gend)),xaxt=\"n\");
axis(1,xaxp=c(as.Date(gstart),as.Date(gend),pertick),at=seq(as.Date(gstart),as.Date(gend),by=pertick),labels=c(dlabels));
lines(segplot,col=\"blue\",type=\"l\");

#4,1
query<-paste(\"WITH dm as (SELECT date_trunc('day', ac1.epochtime), max(ac1.epochtime) epochtime, ac1.cid, (SELECT count(distinct cid) FROM \",arg2,\"_arc_cid where \",arg2,\"_arc_cid.cid = ac1.cid and \",arg2,\"_arc_cid.epochtime=max(ac1.epochtime) ) as numcid from \",arg2,\"_arc_cid ac1 where epochtime between (now() - '\",arg3,\" days'::interval) and (now() - '6 hours'::interval) group by 1,3) SELECT dm.date_trunc::date, sum(dm.numcid) from dm group by 1 order by 1\",sep=\"\");
segplot<-pg.spi.exec(query);
segplot\$date_trunc<-as.Date(segplot\$date_trunc);
plot(segplot,col=\"red\",pch=20,type=\"p\",xlab=\"date\",ylab=\"number of cids\",main=\"Number of CIDs\",xlim=c(as.Date(gstart),as.Date(gend)),xaxt=\"n\");
axis(1,xaxp=c(as.Date(gstart),as.Date(gend),pertick),at=seq(as.Date(gstart),as.Date(gend),by=pertick),labels=c(dlabels));
lines(segplot,col=\"blue\",type=\"l\");

#4,2
query<-paste(\"WITH dm as (SELECT date_trunc('day', ac1.epochtime), max(ac1.epochtime) epochtime, ac1.cid, (SELECT usrcnt FROM \",arg2,\"_arc_cid where \",arg2,\"_arc_cid.cid = ac1.cid and \",arg2,\"_arc_cid.epochtime=max(ac1.epochtime) ) as usrcnt from \",arg2,\"_arc_cid ac1 where epochtime between (now() - '\",arg3,\" days'::interval) and (now() - '6 hours'::interval) group by 1,3) SELECT dm.date_trunc::date, sum(dm.usrcnt) from dm group by 1 order by 1\",sep=\"\");
segplot<-pg.spi.exec(query);
segplot\$date_trunc<-as.Date(segplot\$date_trunc);
plot(segplot,col=\"red\",pch=20,type=\"p\",xlab=\"date\",ylab=\"number of users\",main=\"Number of users\",xlim=c(as.Date(gstart),as.Date(gend)),xaxt=\"n\");
axis(1,xaxp=c(as.Date(gstart),as.Date(gend),pertick),at=seq(as.Date(gstart),as.Date(gend),by=pertick),labels=c(dlabels));
lines(segplot,col=\"blue\",type=\"l\");

#4,3
query<-paste(\"WITH dm as (SELECT date_trunc('day', ac1.epochtime), max(ac1.epochtime) epochtime, ac1.cid, (SELECT masrej FROM \",arg2,\"_arc_cid where \",arg2,\"_arc_cid.cid = ac1.cid and \",arg2,\"_arc_cid.epochtime=max(ac1.epochtime) ) as masrej from \",arg2,\"_arc_cid ac1 where epochtime between (now() - '\",arg3,\" days'::interval) and (now() - '6 hours'::interval) group by 1,3) SELECT dm.date_trunc::date, sum(dm.masrej) from dm group by 1 order by 1\",sep=\"\");
segplot<-pg.spi.exec(query);
segplot\$date_trunc<-as.Date(segplot\$date_trunc);
plot(segplot,col=\"red\",pch=20,type=\"p\",xlab=\"date\",ylab=\"mas rejects\",main=\"MAS Rejects\",xlim=c(as.Date(gstart),as.Date(gend)),xaxt=\"n\");
axis(1,xaxp=c(as.Date(gstart),as.Date(gend),pertick),at=seq(as.Date(gstart),as.Date(gend),by=pertick),labels=c(dlabels));
lines(segplot,col=\"blue\",type=\"l\");

#4,4
#

title(paste(\"Summation over all Segments - \",arg2,\" \",sep=\"\"),sub = paste(\"Generated:\",date(),\" \",sep=\"\"),outer=TRUE, col.main= \"red\", font.sub = 30, col.sub = \"blue\");

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

#
# Individual server pair data
#
$dbh = DBI->connect("dbi:Pg:dbname=watcher;host=localhost","postgres","") || die $DBI::errstr;

if ( $flags{'verbose'} )
{
	print "\nconnect status in dbh for arc_srv_detail_plot\n";
	print Dumper $dbh;
}

                  #arg1     arg2    arg3     arg4     arg5               arg6              arg7
                 #'$region', $tspan, '$loc1', '$loc2', '$solrips1[$num]', '$solrips2[$num] $zoom'
$psqlcmd = "create or replace function arc_srv_detail_plot(varchar, integer, varchar, varchar, varchar, varchar, integer) returns void as \$\$

filename<-paste(\"\",arg1,\"-\",arg5,\"-\",arg6,\"-\",arg2,\"-\",arg7,\".png\",sep=\"\");
ffilename<-paste(\"\",\"var\",\"www\",\"html\",\"watcher\",\"archiving\",filename,sep=\"/\");
png(ffilename,width = 1200*arg7,height = 1000*arg7,units = \"px\",pointsize = 16);

gstart<-format((Sys.time() - ((3600*24)*arg2)), \"%Y-%m-%d\");
gend<-format(Sys.time(), \"%Y-%m-%d\");

ticks<-arg2;
if (arg2 == 30)  switch(arg7,ticks<-5,ticks<-10,ticks<-15,ticks<-15,ticks<-30);
if (arg2 == 60)  switch(arg7,ticks<-5,ticks<-12,ticks<-20,ticks<-20,ticks<-30);
if (arg2 == 90)  switch(arg7,ticks<-5,ticks<-12,ticks<-18,ticks<-22,ticks<-30);
if (arg2 == 180) switch(arg7,ticks<-5,ticks<-12,ticks<-20,ticks<-25,ticks<-30);

pertick<-as.integer(arg2/ticks);

dlabels<-c();
for ( x in (ticks:0))
{
   lbint<-c(format(Sys.time() - (((3600*24)*arg2)*(x/ticks)), \"%m-%d\"));
   dlabels<-c(c(dlabels), c(lbint));
}

par(mfrow=c(2,3),oma=c(5,0,3,0),cex.main=2);

# 1,1
query<<-paste(\"WITH dm as (SELECT date_trunc('day', ac1.epochtime), max(ac1.epochtime) epochtime, ac1.srv_ip, (SELECT dnscnt FROM \",arg1,\"_arc_server where \",arg1,\"_arc_server.srv_ip = ac1.srv_ip and \",arg1,\"_arc_server.epochtime=max(ac1.epochtime) and srv_ip='\",arg5,\"' ) as dnscnt from \",arg1,\"_arc_server ac1 where epochtime between (now() - '\",arg2,\" days'::interval) and (now() - '6 hours'::interval) group by 1,3) SELECT dm.date_trunc::date, sum(dm.dnscnt) from dm group by 1 order by 1\",sep=\"\");
segplot<<-pg.spi.exec(query);
segplot\$date_trunc<-as.Date(segplot\$date_trunc);
plot(segplot,col=\"red\",pch=20,type=\"p\",xlab=\"date\",ylab=\"DNSdirector count\",main=paste(\"\",arg3,\" DNSdirector count\",sep=\"\"),xlim=c(as.Date(gstart),as.Date(gend)),xaxt=\"n\");
axis(1,xaxp=c(as.Date(gstart),as.Date(gend),pertick),at=seq(as.Date(gstart),as.Date(gend),by=pertick),labels=c(dlabels));
lines(segplot,col=\"blue\",type=\"l\");

# 1,2
query<-paste(\"WITH dm as (SELECT date_trunc('day', ac1.epochtime), max(ac1.epochtime) epochtime, ac1.srv_ip, (SELECT dircnt FROM \",arg1,\"_arc_server where \",arg1,\"_arc_server.srv_ip = ac1.srv_ip and \",arg1,\"_arc_server.epochtime=max(ac1.epochtime) and srv_ip='\",arg5,\"' ) as dircnt from \",arg1,\"_arc_server ac1 where epochtime between (now() - '\",arg2,\" days'::interval) and (now() - '6 hours'::interval) group by 1,3) SELECT dm.date_trunc::date, sum(dm.dircnt) from dm group by 1 order by 1\",sep=\"\");
segplot<-pg.spi.exec(query);
segplot\$date_trunc<-as.Date(segplot\$date_trunc);
plot(segplot,col=\"red\",pch=20,type=\"p\",xlab=\"date\",ylab=\"Disk dir count\",main=paste(\"\",arg3,\" Disk dir count\",sep=\"\"),xlim=c(as.Date(gstart),as.Date(gend)),xaxt=\"n\");
axis(1,xaxp=c(as.Date(gstart),as.Date(gend),pertick),at=seq(as.Date(gstart),as.Date(gend),by=pertick),labels=c(dlabels));
lines(segplot,col=\"blue\",type=\"l\");

# 1,3
# get the data for the free disk line
query<-paste(\"WITH dm as (SELECT date_trunc('day', ac1.epochtime), max(ac1.epochtime) epochtime, ac1.srv_ip, (SELECT diskfree FROM \",arg1,\"_arc_server where \",arg1,\"_arc_server.srv_ip = ac1.srv_ip and \",arg1,\"_arc_server.epochtime=max(ac1.epochtime) and srv_ip='\",arg5,\"' )/1024.0 as diskfree from \",arg1,\"_arc_server ac1 where epochtime between (now() - '\",arg2,\" days'::interval) and (now() - '6 hours'::interval) group by 1,3) SELECT dm.date_trunc::date, coalesce(sum(dm.diskfree),0) as sum from dm group by 1 order by 1\",sep=\"\");
segplot<-pg.spi.exec(query);
segplot\$date_trunc<-as.Date(segplot\$date_trunc);

# get min and max y axis for this graph
query<-paste(\"WITH dn as (WITH dm as (SELECT date_trunc('day', ac1.epochtime), max(ac1.epochtime) epochtime, ac1.srv_ip, (SELECT diskfree FROM \",arg1,\"_arc_server where \",arg1,\"_arc_server.srv_ip = ac1.srv_ip and \",arg1,\"_arc_server.epochtime=max(ac1.epochtime) and srv_ip='\",arg5,\"' )/1024.0 as diskfree from \",arg1,\"_arc_server ac1 where epochtime between (now() - '\",arg2,\" days'::interval) and (now() - '6 hours'::interval) group by 1,3) SELECT dm.date_trunc::date, coalesce(sum(dm.diskfree),0) as sum from dm group by 1 order by 1) select sum from dn\",sep=\"\");
dmin<-min(pg.spi.exec(query));
dmax<-max(pg.spi.exec(query));

###pg.thrownotice(query);
###pg.thrownotice(dmin);
###pg.thrownotice(dmax);

# get the solr label that this IP address is in
query<-paste(\"select distinct(solrlabel) from \",arg1,\"_arc_cid where '\",arg5,\"' = ANY (solrips) and epochtime between (now() - '\",arg2,\" days'::interval) and (now() - '6 hours'::interval) limit 1\",sep=\"\");
slabel<-pg.spi.exec(query);
###loct<-unlist(strsplit(arg5,split=\".\",fixed=TRUE))[[4]];

# get the index for the solr IP address so we can index into the free disk and solr sizes
query<-paste(\"select solrips from \",arg1,\"_arc_cid where '\",arg5,\"' = ANY (solrips) and epochtime between (now() - '\",arg2,\" days'::interval) and (now() - '6 hours'::interval) order by epochtime desc limit 1\",sep=\"\");
solrips<-unlist(pg.spi.exec(query));

###pg.thrownotice(length(solrips));
###pg.thrownotice(arg5);
###pg.thrownotice(solrips);
###pg.thrownotice(solrips[1]);
###pg.thrownotice(solrips[2]);

for (mbindex in 1:length(solrips))
{
	###pg.thrownotice(solrips[mbindex]);

	if ( arg5 == solrips[mbindex] )
	{
		index<-mbindex
	}
}

###pg.thrownotice(index);

# get largest core for this solr label x 0.5
query<-paste(\"SELECT date_trunc('day', epochtime)::date, ((max(coalesce(sizemb[\",index,\"],0))*0.5)/1024.0) as maxi from \",arg1,\"_arc_cid where solrlabel ~'\",slabel,\"' and epochtime between (now() - '\",arg2,\" days'::interval) and (now() - '6 hours'::interval) group by 1 order by 1\",sep=\"\");
segplot2<-pg.spi.exec(query);
segplot2\$date_trunc<-as.Date(segplot2\$date_trunc);
 
# get min and max y axis value for this graph
query<-paste(\"WITH dm as (SELECT date_trunc('day', epochtime)::date, ((max(coalesce(sizemb[\",index,\"],0))*0.5)/1024.0) as maxi from \",arg1,\"_arc_cid where solrlabel ~'\",slabel,\"' and epochtime between (now() - '\",arg2,\" days'::interval) and (now() - '6 hours'::interval) group by 1 order by 1) select dm.maxi from dm\",sep=\"\");
omin<-min(pg.spi.exec(query),na.rm = TRUE);
omax<-max(pg.spi.exec(query),na.rm = TRUE);

###pg.thrownotice(query);
###pg.thrownotice(omin);
###pg.thrownotice(omax);

#### # get largest core for this solr label x 0.5
#### query<-paste(\"SELECT date_trunc('day', epochtime)::date, ((max(sizemb1)*0.5)/1024.0) as maxi from \",arg1,\"_arc_cid where solrlabel ~'\",slabel,\"' and epochtime between (now() - '\",arg2,\" days'::interval) and (now() - '6 hours'::interval) group by 1 order by 1\",sep=\"\");
#### segplot2<-pg.spi.exec(query);
#### segplot2\$date_trunc<-as.Date(segplot2\$date_trunc);
#### 
#### # get min and max y axis value for this graph
#### query<-paste(\"WITH dm as (SELECT date_trunc('day', epochtime)::date, ((max(sizemb1)*0.5)/1024.0) as maxi from \",arg1,\"_arc_cid where solrlabel ~'\",slabel,\"' and epochtime between (now() - '\",arg2,\" days'::interval) and (now() - '6 hours'::interval) group by 1 order by 1) select dm.maxi from dm\",sep=\"\");
#### omin<-min(pg.spi.exec(query));
#### omax<-max(pg.spi.exec(query));

# set the absolute y axis min and max values 
ymin<-min(omin,dmin)*0.8;
ymax<-max(omax,dmax);

###pg.thrownotice(ymin);
###pg.thrownotice(ymax);

plot(segplot,lty=1,lwd=2,type=\"l\",pch=20,col=\"blue\",xlab=\"date\",ylab=\"Free disk space (GB)\",ylim=c(ymin,ymax),main=paste(\"\",arg3,\" Free disk space\",sep=\"\"),xlim=c(as.Date(gstart),as.Date(gend)),xaxt=\"n\");
par(\"new\"=TRUE); 
plot(segplot2,lty=2,lwd=2,type=\"l\",pch=20,col=\"red\",xlab=\"date\",ylab=\"Free disk space (GB)\",ylim=c(ymin,ymax),main=paste(\"\",arg3,\" Free disk space\",sep=\"\"),xlim=c(as.Date(gstart),as.Date(gend)),xaxt=\"n\");
axis(1,xaxp=c(as.Date(gstart),as.Date(gend),pertick),at=seq(as.Date(gstart),as.Date(gend),by=pertick),labels=c(dlabels));
legend(\"bottom\",c(\"Available\",\"Required to Optimize\"),col=c(\"blue\",\"red\"),lty=c(1,2));

# 2,1
query<-paste(\"WITH dm as (SELECT date_trunc('day', ac1.epochtime), max(ac1.epochtime) epochtime, ac1.srv_ip, (SELECT dnscnt FROM \",arg1,\"_arc_server where \",arg1,\"_arc_server.srv_ip = ac1.srv_ip and \",arg1,\"_arc_server.epochtime=max(ac1.epochtime) and srv_ip='\",arg5,\"' ) as dnscnt from \",arg1,\"_arc_server ac1 where epochtime between (now() - '\",arg2,\" days'::interval) and (now() - '6 hours'::interval) group by 1,3) SELECT dm.date_trunc::date, sum(dm.dnscnt) from dm group by 1 order by 1\",sep=\"\");
segplot<-pg.spi.exec(query);
segplot\$date_trunc<-as.Date(segplot\$date_trunc);
plot(segplot,col=\"red\",pch=20,type=\"p\",xlab=\"date\",ylab=\"DNSdirector count\",main=paste(\"\",arg4,\" DNSdirector count\",sep=\"\"),xlim=c(as.Date(gstart),as.Date(gend)),xaxt=\"n\");
axis(1,xaxp=c(as.Date(gstart),as.Date(gend),pertick),at=seq(as.Date(gstart),as.Date(gend),by=pertick),labels=c(dlabels));
lines(segplot,col=\"blue\",type=\"l\");

# 2,2
query<-paste(\"WITH dm as (SELECT date_trunc('day', ac1.epochtime), max(ac1.epochtime) epochtime, ac1.srv_ip, (SELECT dircnt FROM \",arg1,\"_arc_server where \",arg1,\"_arc_server.srv_ip = ac1.srv_ip and \",arg1,\"_arc_server.epochtime=max(ac1.epochtime) and srv_ip='\",arg5,\"' ) as dircnt from \",arg1,\"_arc_server ac1 where epochtime between (now() - '\",arg2,\" days'::interval) and (now() - '6 hours'::interval) group by 1,3) SELECT dm.date_trunc::date, sum(dm.dircnt) from dm group by 1 order by 1\",sep=\"\");
segplot<-pg.spi.exec(query);
segplot\$date_trunc<-as.Date(segplot\$date_trunc);
plot(segplot,col=\"red\",pch=20,type=\"p\",xlab=\"date\",ylab=\"Disk dir count\",main=paste(\"\",arg4,\" Disk dir count\",sep=\"\"),xlim=c(as.Date(gstart),as.Date(gend)),xaxt=\"n\");
axis(1,xaxp=c(as.Date(gstart),as.Date(gend),pertick),at=seq(as.Date(gstart),as.Date(gend),by=pertick),labels=c(dlabels));
lines(segplot,col=\"blue\",type=\"l\");

# 2,3
# get the data for the free disk line
query<-paste(\"WITH dm as (SELECT date_trunc('day', ac1.epochtime), max(ac1.epochtime) epochtime, ac1.srv_ip, (SELECT diskfree FROM \",arg1,\"_arc_server where \",arg1,\"_arc_server.srv_ip = ac1.srv_ip and \",arg1,\"_arc_server.epochtime=max(ac1.epochtime) and srv_ip='\",arg6,\"' )/1024.0 as diskfree from \",arg1,\"_arc_server ac1 where epochtime between (now() - '\",arg2,\" days'::interval) and (now() - '6 hours'::interval) group by 1,3) SELECT dm.date_trunc::date, coalesce(sum(dm.diskfree),0) as sum from dm group by 1 order by 1\",sep=\"\");
segplot<-pg.spi.exec(query);
segplot\$date_trunc<-as.Date(segplot\$date_trunc);

# get min and max y axis for this graph
query<-paste(\"WITH dn as (WITH dm as (SELECT date_trunc('day', ac1.epochtime), max(ac1.epochtime) epochtime, ac1.srv_ip, (SELECT diskfree FROM \",arg1,\"_arc_server where \",arg1,\"_arc_server.srv_ip = ac1.srv_ip and \",arg1,\"_arc_server.epochtime=max(ac1.epochtime) and srv_ip='\",arg6,\"' )/1024.0 as diskfree from \",arg1,\"_arc_server ac1 where epochtime between (now() - '\",arg2,\" days'::interval) and (now() - '6 hours'::interval) group by 1,3) SELECT dm.date_trunc::date, coalesce(sum(dm.diskfree),0) as sum from dm group by 1 order by 1) select sum from dn\",sep=\"\");
dmin<-min(pg.spi.exec(query));
dmax<-max(pg.spi.exec(query));

# get the solr label that this IP address is in
query<-paste(\"select distinct(solrlabel) from \",arg1,\"_arc_cid where '\",arg6,\"' = ANY (solrips) and epochtime between (now() - '\",arg2,\" days'::interval) and (now() - '6 hours'::interval) limit 1\",sep=\"\");
slabel<-pg.spi.exec(query);
###loct<-unlist(strsplit(arg6,split=\".\",fixed=TRUE))[[4]];

# get the index for the solr IP address so we can index into the free disk and solr sizes
query<-paste(\"select solrips from \",arg1,\"_arc_cid where '\",arg6,\"' = ANY (solrips) and epochtime between (now() - '\",arg2,\" days'::interval) and (now() - '6 hours'::interval) order by epochtime desc limit 1\",sep=\"\");
solrips<-unlist(pg.spi.exec(query));

for (mbindex in 1:length(solrips))
{
	###pg.thrownotice(solrips[mbindex]);

	if ( arg6 == solrips[mbindex] )
	{
		index<-mbindex
	}
}

###pg.thrownotice(index);

###pg.thrownotice(query);

# get largest core for this solr label x 0.5
query<-paste(\"SELECT date_trunc('day', epochtime)::date, ((max(coalesce(sizemb[\",index,\"]))*0.5)/1024.0) as maxi from \",arg1,\"_arc_cid where solrlabel ~'\",slabel,\"' and epochtime between (now() - '\",arg2,\" days'::interval) and (now() - '6 hours'::interval) group by 1 order by 1\",sep=\"\");
segplot2<-pg.spi.exec(query);
segplot2\$date_trunc<-as.Date(segplot2\$date_trunc);
 
###pg.thrownotice(query);

# get min and max y axis value for this graph
query<-paste(\"WITH dm as (SELECT date_trunc('day', epochtime)::date, ((max(coalesce(sizemb[\",index,\"]))*0.5)/1024.0) as maxi from \",arg1,\"_arc_cid where solrlabel ~'\",slabel,\"' and epochtime between (now() - '\",arg2,\" days'::interval) and (now() - '6 hours'::interval) group by 1 order by 1) select dm.maxi from dm\",sep=\"\");
omin<-min(pg.spi.exec(query),na.rm = TRUE);
omax<-max(pg.spi.exec(query),na.rm = TRUE);

#### # get largest core for this solr label x 0.5
#### query<-paste(\"SELECT date_trunc('day', epochtime)::date, ((max(sizemb2)*0.5)/1024.0) as maxi from \",arg1,\"_arc_cid where solrlabel ~'\",slabel,\"' and epochtime between (now() - '\",arg2,\" days'::interval) and (now() - '6 hours'::interval) group by 1 order by 1\",sep=\"\");
#### segplot2<-pg.spi.exec(query);
#### segplot2\$date_trunc<-as.Date(segplot2\$date_trunc);
#### 
#### # get min and max y axis value for this graph
#### query<-paste(\"WITH dm as (SELECT date_trunc('day', epochtime)::date, ((max(sizemb2)*0.5)/1024.0) as maxi from \",arg1,\"_arc_cid where solrlabel ~'\",slabel,\"' and epochtime between (now() - '\",arg2,\" days'::interval) and (now() - '6 hours'::interval) group by 1 order by 1) select dm.maxi from dm\",sep=\"\");
#### omin<-min(pg.spi.exec(query));
#### omax<-max(pg.spi.exec(query));

###pg.thrownotice(query);

# set the absolute y axis min and max values 
ymin<-min(omin,dmin)*0.8;
ymax<-max(omax,dmax);

plot(segplot,lty=1,lwd=2,type=\"l\",pch=20,col=\"blue\",xlab=\"date\",ylab=\"Free disk space (GB)\",ylim=c(ymin,ymax),main=paste(\"\",arg4,\" Free disk space\",sep=\"\"),xlim=c(as.Date(gstart),as.Date(gend)),xaxt=\"n\");
par(\"new\"=TRUE); 
plot(segplot2,lty=2,lwd=2,type=\"l\",pch=20,col=\"red\",xlab=\"date\",ylab=\"Free disk space (GB)\",ylim=c(ymin,ymax),main=paste(\"\",arg4,\" Free disk space\",sep=\"\"),xlim=c(as.Date(gstart),as.Date(gend)),xaxt=\"n\");
axis(1,xaxp=c(as.Date(gstart),as.Date(gend),pertick),at=seq(as.Date(gstart),as.Date(gend),by=pertick),labels=c(dlabels));
legend(\"bottom\",c(\"Available\",\"Required to Optimize\"),col=c(\"blue\",\"red\"),lty=c(1,2));


title(paste(\"Server pairs \",arg5,\"-\",arg6,\" detailed data - \",arg1,\" \",sep=\"\"),sub = paste(\"Generated:\",date(),\" \",sep=\"\"),outer=TRUE, col.main= \"red\", font.sub = 30, col.sub = \"blue\");

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

#
# Summary server data
#
$dbh = DBI->connect("dbi:Pg:dbname=watcher;host=localhost","postgres","") || die $DBI::errstr;

if ( $flags{'verbose'} )
{
	print "\nconnect status in dbh\n";
	print Dumper $dbh;
}

#													arc_srv_summary_plot('$region', $tspan, '$loc1', '$loc2', $zoom)
$psqlcmd = "create or replace function arc_srv_summary_plot(varchar, integer, varchar, varchar, integer) returns void as \$\$

filename<-paste(\"\",arg1,\"-sum-servers-\",arg2,\"-\",arg5,\".png\",sep=\"\");
ffilename<-paste(\"\",\"var\",\"www\",\"html\",\"watcher\",\"archiving\",filename,sep=\"/\");
png(ffilename,width = 1200*arg5,height = 1000*arg5,units = \"px\",pointsize = 16);

gstart<-format((Sys.time() - ((3600*24)*arg2)), \"%Y-%m-%d\");
gend<-format(Sys.time(), \"%Y-%m-%d\");

ticks<-arg2;
if (arg2 == 30)  switch(arg5,ticks<-5,ticks<-10,ticks<-15,ticks<-15,ticks<-30);
if (arg2 == 60)  switch(arg5,ticks<-5,ticks<-12,ticks<-20,ticks<-20,ticks<-30);
if (arg2 == 90)  switch(arg5,ticks<-5,ticks<-12,ticks<-18,ticks<-22,ticks<-30);
if (arg2 == 180) switch(arg5,ticks<-5,ticks<-12,ticks<-20,ticks<-25,ticks<-30);

pertick<-as.integer(arg2/ticks);

###pg.thrownotice(ticks);
###pg.thrownotice(pertick);

dlabels<-c();
for ( x in (ticks:0)) 
{
	lbint<-c(format(Sys.time() - (((3600*24)*arg2)*(x/ticks)), \"%m-%d\"));
	dlabels<-c(c(dlabels), c(lbint));
}

par(mfrow=c(2,3),oma=c(5,0,3,0),cex.main=2);

# 1,1
query<<-paste(\"WITH dm as (SELECT date_trunc('day', ac1.epochtime), max(ac1.epochtime) epochtime, ac1.srv_ip, (SELECT dnscnt FROM \",arg1,\"_arc_server where \",arg1,\"_arc_server.srv_ip = ac1.srv_ip and \",arg1,\"_arc_server.epochtime=max(ac1.epochtime) ) as dnscnt from \",arg1,\"_arc_server ac1 where epochtime between (now() - '\",arg2,\" days'::interval) and (now() - '6 hours'::interval) and datacenter = '\",arg3,\"' group by 1,3) SELECT dm.date_trunc::date, sum(dm.dnscnt) from dm group by 1 order by 1\",sep=\"\");
segplot<<-pg.spi.exec(query);
segplot\$date_trunc<-as.Date(segplot\$date_trunc);
plot(segplot,col=\"red\",pch=20,type=\"p\",xlab=\"date\",ylab=\"DNSdirector count\",main=paste(\"\",arg3,\" DNSdirector count\",sep=\"\"),xlim=c(as.Date(gstart),as.Date(gend)),xaxt=\"n\");
axis(1,xaxp=c(as.Date(gstart),as.Date(gend),pertick),at=seq(as.Date(gstart),as.Date(gend),by=pertick),labels=c(dlabels));
lines(segplot,col=\"blue\",type=\"l\");

# 1,2
query<-paste(\"WITH dm as (SELECT date_trunc('day', ac1.epochtime), max(ac1.epochtime) epochtime, ac1.srv_ip, (SELECT dircnt FROM \",arg1,\"_arc_server where \",arg1,\"_arc_server.srv_ip = ac1.srv_ip and \",arg1,\"_arc_server.epochtime=max(ac1.epochtime) ) as dircnt from \",arg1,\"_arc_server ac1 where epochtime between (now() - '\",arg2,\" days'::interval) and (now() - '6 hours'::interval) and datacenter = '\",arg3,\"' group by 1,3) SELECT dm.date_trunc::date, sum(dm.dircnt) from dm group by 1 order by 1\",sep=\"\");
segplot<-pg.spi.exec(query);
segplot\$date_trunc<-as.Date(segplot\$date_trunc);
plot(segplot,col=\"red\",pch=20,type=\"p\",xlab=\"date\",ylab=\"Disk dir count\",main=paste(\"\",arg3,\" Disk dir count\",sep=\"\"),xlim=c(as.Date(gstart),as.Date(gend)),xaxt=\"n\");
axis(1,xaxp=c(as.Date(gstart),as.Date(gend),pertick),at=seq(as.Date(gstart),as.Date(gend),by=pertick),labels=c(dlabels));
lines(segplot,col=\"blue\",type=\"l\");

# 1,3
query<-paste(\"WITH dm as (SELECT date_trunc('day', ac1.epochtime), max(ac1.epochtime) epochtime, ac1.srv_ip, (SELECT diskfree FROM \",arg1,\"_arc_server where \",arg1,\"_arc_server.srv_ip = ac1.srv_ip and \",arg1,\"_arc_server.epochtime=max(ac1.epochtime) )/1024.0 as diskfree from \",arg1,\"_arc_server ac1 where epochtime between (now() - '\",arg2,\" days'::interval) and (now() - '6 hours'::interval) and datacenter = '\",arg3,\"' group by 1,3) SELECT dm.date_trunc::date, sum(dm.diskfree) from dm group by 1 order by 1\",sep=\"\");
segplot<-pg.spi.exec(query);
segplot\$date_trunc<-as.Date(segplot\$date_trunc);

plot(segplot,col=\"red\",pch=20,type=\"p\",xlab=\"date\",ylab=\"Free disk space (GB)\",main=paste(\"\",arg3,\" Free disk space\",sep=\"\"),xlim=c(as.Date(gstart),as.Date(gend)),xaxt=\"n\");
axis(1,xaxp=c(as.Date(gstart),as.Date(gend),pertick),at=seq(as.Date(gstart),as.Date(gend),by=pertick),labels=c(dlabels));
lines(segplot,col=\"blue\",type=\"l\");

# 2,1
query<-paste(\"WITH dm as (SELECT date_trunc('day', ac1.epochtime), max(ac1.epochtime) epochtime, ac1.srv_ip, (SELECT dnscnt FROM \",arg1,\"_arc_server where \",arg1,\"_arc_server.srv_ip = ac1.srv_ip and \",arg1,\"_arc_server.epochtime=max(ac1.epochtime) ) as dnscnt from \",arg1,\"_arc_server ac1 where epochtime between (now() - '\",arg2,\" days'::interval) and (now() - '6 hours'::interval) and datacenter = '\",arg4,\"' group by 1,3) SELECT dm.date_trunc::date, sum(dm.dnscnt) from dm group by 1 order by 1\",sep=\"\");
segplot<-pg.spi.exec(query);
segplot\$date_trunc<-as.Date(segplot\$date_trunc);
plot(segplot,col=\"red\",pch=20,type=\"p\",xlab=\"date\",ylab=\"DNSdirector count\",main=paste(\"\",arg4,\" DNSdirector count\",sep=\"\"),xlim=c(as.Date(gstart),as.Date(gend)),xaxt=\"n\");
axis(1,xaxp=c(as.Date(gstart),as.Date(gend),pertick),at=seq(as.Date(gstart),as.Date(gend),by=pertick),labels=c(dlabels));
lines(segplot,col=\"blue\",type=\"l\");

# 2,2
query<-paste(\"WITH dm as (SELECT date_trunc('day', ac1.epochtime), max(ac1.epochtime) epochtime, ac1.srv_ip, (SELECT dircnt FROM \",arg1,\"_arc_server where \",arg1,\"_arc_server.srv_ip = ac1.srv_ip and \",arg1,\"_arc_server.epochtime=max(ac1.epochtime) ) as dircnt from \",arg1,\"_arc_server ac1 where epochtime between (now() - '\",arg2,\" days'::interval) and (now() - '6 hours'::interval) and datacenter = '\",arg4,\"' group by 1,3) SELECT dm.date_trunc::date, sum(dm.dircnt) from dm group by 1 order by 1\",sep=\"\");
segplot<-pg.spi.exec(query);
segplot\$date_trunc<-as.Date(segplot\$date_trunc);
plot(segplot,col=\"red\",pch=20,type=\"p\",xlab=\"date\",ylab=\"Disk dir count\",main=paste(\"\",arg4,\"Disk dir count\",sep=\"\"),xlim=c(as.Date(gstart),as.Date(gend)),xaxt=\"n\");
axis(1,xaxp=c(as.Date(gstart),as.Date(gend),pertick),at=seq(as.Date(gstart),as.Date(gend),by=pertick),labels=c(dlabels));
lines(segplot,col=\"blue\",type=\"l\");

# 2,3
query<-paste(\"WITH dm as (SELECT date_trunc('day', ac1.epochtime), max(ac1.epochtime) epochtime, ac1.srv_ip, (SELECT diskfree FROM \",arg1,\"_arc_server where \",arg1,\"_arc_server.srv_ip = ac1.srv_ip and \",arg1,\"_arc_server.epochtime=max(ac1.epochtime) )/1024.0 as diskfree from \",arg1,\"_arc_server ac1 where epochtime between (now() - '\",arg2,\" days'::interval) and (now() - '6 hours'::interval) and datacenter = '\",arg4,\"' group by 1,3) SELECT dm.date_trunc::date, sum(dm.diskfree) from dm group by 1 order by 1\",sep=\"\");
segplot<-pg.spi.exec(query);
segplot\$date_trunc<-as.Date(segplot\$date_trunc);

plot(segplot,col=\"red\",pch=20,type=\"p\",xlab=\"date\",ylab=\"Free disk space (GB)\",main=paste(\"\",arg4,\" Free disk space\",sep=\"\"),xlim=c(as.Date(gstart),as.Date(gend)),xaxt=\"n\");
axis(1,xaxp=c(as.Date(gstart),as.Date(gend),pertick),at=seq(as.Date(gstart),as.Date(gend),by=pertick),labels=c(dlabels));
lines(segplot,col=\"blue\",type=\"l\");


title(paste(\"Server pairs summary data - \",arg1,\" \",sep=\"\"),sub = paste(\"Generated:\",date(),\" \",sep=\"\"),outer=TRUE, col.main= \"red\", font.sub = 30, col.sub = \"blue\");

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
## Now call the various functions with all appropriate data for each picture
##
foreach $region (@regions)
{
	#switch ($region)
	#{
		#case "US1"
		#{
			#$loc1="Englewood";
			#$loc2="Denver";
		#}
		#case "US2"
		#{
			#$loc1="Englewood";
			#$loc2="Denver";
		#}
	#}

	foreach $tspan (@tspans)
	{
		foreach $zoom (1..5)
		{
			$dest = "$region" . "_arc_cid";

			$destsrv = "$region" . "_arc_server";

			@lsegs = `/usr/bin/psql -At -U postgres watcher -c "select distinct(solrlabel) from $dest where epochtime between (now() - '$tspan days'::interval) and (now() - '6 hours'::interval)"`;
			chomp(@lsegs);

#
# Make the list of long segments unique
#
   		undef %seen;
   		@lsegs = grep(!$seen{$_}++, @lsegs);

			if ( $flags{'verbose'} )
			{
				print "\nlong segments\n";
				print Dumper \@lsegs;
			}

			@ssegs = ();

			foreach $seg (@lsegs)
      	{
				if ( $seg ne "" )
				{
					@list = split("\\.",$seg);
					push(@ssegs,$list[0]);
				}
			}

			chomp(@ssegs);

#
# Make the list of short segments unique
#
   		undef %seen;
   		@ssegs = grep(!$seen{$_}++, @ssegs);
	
			if ( $flags{'verbose'} )
			{
				print "\nshort segments\n";
				print Dumper \@ssegs;
			}

			@locs = `/usr/bin/psql -At -U postgres watcher -c "select distinct datacenter from $destsrv where epochtime between (now() - '160 days'::interval) and now() order by datacenter desc"`;
			chomp(@locs);


			$site1 = (split(/\./,`/usr/bin/psql -At -U postgres watcher -c "select distinct(srv_ip) from $destsrv where epochtime between (now() - '160 days'::interval) and now() and datacenter='$locs[0]' limit 1"`))[1];

			$site2 = (split(/\./,`/usr/bin/psql -At -U postgres watcher -c "select distinct(srv_ip) from $destsrv where epochtime between (now() - '160 days'::interval) and now() and datacenter='$locs[1]' limit 1"`))[1];

			if ( $flags{'verbose'} )
			{
				print "\nsite 1 number\n";
				print Dumper $site1;
				print "\nsite 2 number\n";
				print Dumper $site2;
			}

			if ( $site1 <= $site2 )
			{
				$loc1 = $locs[0];
				chomp($loc1);

				$loc2 = $locs[1];
				chomp($loc2);
			}
			else
			{
				$loc1 = $locs[1];
				chomp($loc1);

				$loc2 = $locs[0];
				chomp($loc2);
			}

			if ( $flags{'verbose'} )
			{
				print "\nLocation 1\n";
				print Dumper $loc1;
				print "\nLocation 2\n";
				print Dumper $loc2;
			}

			@solrips1 = `/usr/bin/psql -At -U postgres watcher -c "select distinct(srv_ip) from $destsrv where datacenter='$loc1' and srv_type='solr' and epochtime between (now() - '$tspan days'::interval) and (now() - '6 hours'::interval) order by srv_ip asc"`;

			chomp(@solrips1);

			if ( $flags{'verbose'} )
			{
				print "\nsolr IP addresses for $region and $loc1\n";
				print Dumper \@solrips1;
			}

			@solrips2 = `/usr/bin/psql -At -U postgres watcher -c "select distinct(srv_ip) from $destsrv where datacenter='$loc2' and srv_type='solr' and epochtime between (now() - '$tspan days'::interval) and (now() - '6 hours'::interval) order by srv_ip asc"`;

			chomp(@solrips2);

			if ( $flags{'verbose'} )
			{
				print "solr IP addresses for $region and $loc2\n";
				print Dumper \@solrips2;
			}

#
# Produce the summarys
#
			system("/usr/bin/psql -At -h localhost -U postgres watcher -c \"select arc_srv_summary_plot('$region', $tspan, '$loc1', '$loc2', $zoom)\"") == 0 or die "FATAL ERROR: Could not run function arc_srv_summary_plot \n";

			system("/usr/bin/psql -At -h localhost -U postgres watcher -c \"select arc_solr_plot('', '$region', $tspan, '$loc1', '$loc2', $zoom)\"") == 0 or die "FATAL ERROR: Could not run function arc_solr_plot \n";

#
# Specific data for each segment
#
			for my $num (0 .. $#solrips1)
			{
				print "Printing $region $tspan for $solrips1[$num] and $solrips2[$num]  $loc1 $loc2 Zoom $zoom \n";

				system("/usr/bin/psql -At -h localhost -U postgres watcher -c \"select arc_srv_detail_plot('$region', $tspan, '$loc1', '$loc2', '$solrips1[$num]', '$solrips2[$num]', $zoom)\"") == 0 or die "FATAL ERROR: Could not run function arc_srv_detail_plot \n";
			}

			foreach $seg (@ssegs)
			{
				print "Printing $seg $region $tspan Zoom $zoom\n";
 
				system("/usr/bin/psql -At -h localhost -U postgres watcher -c \"select arc_solr_seg_plot('$seg', '$region', $tspan, '$loc1', '$loc2', $zoom)\"") == 0 or die "FATAL ERROR: Could not run function arc_solr_seg_plot \n";
			}
		}
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

