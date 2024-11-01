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
our $VERSION    = 3.0; # post N update
our $PODCONFIG  = '/mxl/etc/pod_config.xml';

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
my %seen = ();
my $cid = ();
my $window = ();
my $lastrun = ();
my $prevrun = ();
my $hostname = ();

##
## Create variables for solr segment summary
##
my $msstatus = ();
my $mstextstat = ();
my $groupid = ();
my @segments = ();
my @groupids = ();
my @allcids = ();
my @cids = ();
my @cgroupids = ();
my $cgroupid = ();
my @oddindexes = ();
my @evenindexes = ();
my $indexcnt = ();
my $index = ();
my $aindex = ();
my @sids = ();
my $numfront = ();
my $numback = ();
my $solrcount1 = ();
my $solrcount2 = ();
my $solrseq = ();
my $solrseg1 = ();
my $solrseg2 = ();
my $solrcnt1 = ();
my $solrcnt2 = ();
my $sizemb1 = ();
my $sizemb2 = ();
my $mscnt = ();
my $mscount = ();
my $location = ();
my $usrcnt = ();
my $sid = ();
my $usercnt = ();
my $ingestperday = ();
my $solringestperday = ();
my $indexperday = ();
my $ndxbacklg = ();
my $solrej1 = ();
my $solrej2 = ();
my $masrej = ();
my $msbacklg = ();
my $region = ();

##
## Create variables for the server report
##
my $ip = ();
my @serverips = ();
my @srvips = ();
my $dnsdircnt = ();
my $dskdircnt = ();
my $dskfree = ();
my $severe1 = ();
my $severe2 = ();
my $srvtype = ();
my $custnm = ();
my $loc1 = ();
my $loc2 = ();
my $count = ();
my $timestamp = ();
my $Tusercnt = 0;
my $Tnumfront = 0;
my $Tnumback = 0;
my $Tsolrcount1 = 0;
my $Tsolrcount2 = 0;
my $Tsizemb1 = 0;
my $Tsizemb2 = 0;
my $Tmscount = 0;
my $Tndxbacklg = 0;
my $Tsolringestperday = 0;
my $Tindexperday = 0;
my $Tsolrej1 = 0;
my $Tsolrej2 = 0;
my $Tmasrej = 0;
my $Tsevere1 = 0;
my $Tsevere2 = 0;
my $shardtype = 0;

#
# Get the time window based on the end time of the next to last successful run
#
$hostname = `/bin/hostname`;
chomp($hostname);

$lastrun = `/usr/bin/psql -At -U postgres watcher -c "select epochtime from arc_lastrun order by epochtime desc limit 1"`;
chomp($lastrun);

$prevrun = `/usr/bin/psql -At -U postgres watcher -c "select epochtime from arc_lastrun order by epochtime desc limit 1 offset 1"`;
chomp($prevrun);
###$prevrun = `/usr/bin/psql -At -U postgres watcher -c "select extract(epoch from epochtime) from arc_lastrun order by epochtime desc limit 1 offset 1"`;

#
# print out report title
#
$timestamp = localtime(time());

if ( $flags{'support'} )
{
	printf "<HTML>";
	printf "<BODY>";
	printf "<br><br><pre> Archiving report    -   %25s </pre><br><br>", $timestamp ;
	printf "<pre>Watcher data  - From: %25s to %25s  </pre><br><br>", $prevrun, $lastrun ;
}
else
{
	printf "\n\n Archiving report    -   %25s \n\n", $timestamp ;
	printf "Watcher data  - From: %25s to %25s  \n\n", $prevrun, $lastrun ;
}

$ip = `/usr/bin/head -1 /root/dist/archiving_solr`;
chomp($ip);
$loc1 = `/usr/bin/psql -At -U postgres watcher -c "select datacenter from arc_server where srv_ip='$ip' order by epochtime desc limit 1"`;
chomp($loc1);

$loc2 = `/usr/bin/psql -At -U postgres watcher -c "select distinct(datacenter) from arc_server where datacenter!='$loc1' and epochtime between '$prevrun' and '$lastrun'"`;
chomp($loc2);

printf "Locations: %15s / %-15s \n\n", $loc1, $loc2 ;

##
## Get the global information needed
##
$region = `/mxl/sbin/arregion`;
chomp($region);

if ( ! $flags{'support'} )
{
##
## Begin section 1 - solr group ID summary
##
#
# Print out the headings
#
	printf "\nGroup ID   # frontier/back shards    solr count             disk size (MB)      # frontier mail sources  user count   index backlog   ingest per day   index per day  solr rejects      mas rejects        # severe\n\n";
}

#
# Get the list of unique group IDs
#
@groupids=`/usr/bin/psql -At -U postgres watcher -c "select groupid from arc_cid where epochtime between '$prevrun' and '$lastrun'" | /bin/sed s/{//g | /bin/sed s/}//g | /bin/sed s/,/\\\\n/g | /bin/awk '!x[\$0]++'`;
chomp(@groupids);

if(($flags{'debug'}) || ($flags{'verbose'}))
{
	print "\nRegion = $region \n";
	print "\nsolr group IDs\n";
	print Dumper \@groupids;
}

if ( ! $flags{'support'} )
{
#
# Get the data and print out
#

foreach $groupid (@groupids) 
{
#
# Zero all the accumulation counters
#
	$solrcount1 = 0;
	$solrcount2 = 0;
	$sizemb1 = 0;
	$sizemb2 = 0;
	$severe1 = 0;
	$severe2 = 0;
	$solrej1 = 0;
	$solrej2 = 0;

#
# Get all solr IPs for this group ID
#
	@srvips = `/usr/bin/psql -At -U postgres watcher -c "select solrips from arc_cid where epochtime between '$prevrun' and '$lastrun' and ('$groupid' = any(groupid) or fgroupid = '$groupid')" | /bin/sed s/{//g | /bin/sed s/}//g | /bin/sed s/,/\\\\n/g | /bin/awk '!x[\$0]++' `;
	chomp(@srvips);

	if ( $#srvips >= 0 )
	{
		###shift(@srvips);
		push(@serverips, @srvips);
		chomp(@serverips);
	}

	$numfront=`/usr/bin/psql -At -U postgres watcher -c "select count(distinct cid) from arc_cid where fgroupid='$groupid' and epochtime between '$prevrun' and '$lastrun'"`;
	chomp($numfront);
	$Tnumfront += $numfront;
	chomp($Tnumfront);

	$numback=`/usr/bin/psql -At -U postgres watcher -c "select count(distinct cid) from arc_cid where '$groupid' = any(groupid) and fgroupid!='$groupid' and epochtime between '$prevrun' and '$lastrun'"`;
	chomp($numback);
	$Tnumback += $numback;
	chomp($Tnumback);

	@cids = `/usr/bin/psql -At -U postgres watcher -c "select distinct(cid) from arc_cid where epochtime between '$prevrun' and '$lastrun' and ('$groupid' = any(groupid) or fgroupid = '$groupid')"`;
	chomp(@cids);

	foreach $cid (@cids)
	{
		@cgroupids = `/usr/bin/psql -At -U postgres watcher -c "select groupid from arc_cid where epochtime between '$prevrun' and '$lastrun' and cid = $cid" | /bin/sed s/{//g | /bin/sed s/}//g | /bin/sed s/,/\\\\n/g`;
		chomp(@cgroupids);

		if($flags{'debug'})
		{
			print "\nCID = $cid\n";
			print "\ncgroupids\n";
			print Dumper \@cgroupids;
		}

		$indexcnt = 0;
		@oddindexes = ();
		@evenindexes = ();

		foreach $cgroupid (@cgroupids)  #for this cid
		{
			$indexcnt += 1;

			if ( $cgroupid == $groupid )
			{
				push (@oddindexes, ((2*$indexcnt)-1));
				push (@evenindexes, (2*$indexcnt));
			}

		}

		if($flags{'debug'})
		{
			print "\nODD indexes \n";
			print Dumper \@oddindexes;
			print "\nEVEN indexes \n";
			print Dumper \@evenindexes;
		}

		foreach $index (@oddindexes)
		{
			$solrcount1 += `/usr/bin/psql -At -U postgres watcher -c "select solrcnt[$index] from arc_cid where epochtime between '$prevrun' and '$lastrun' and cid=$cid"`;
			chomp($solrcount1);
			if ( $solrcount1 eq "" )
			{
				$solrcount1 = 0;
			}

			$sizemb1 += `/usr/bin/psql -At -U postgres watcher -c "select sizemb[$index] from arc_cid where epochtime between '$prevrun' and '$lastrun' and cid=$cid"`;
			chomp($sizemb1);
			if ( $sizemb1 eq "" )
			{
				$sizemb1 = 0;
			}

			$severe1 += `/usr/bin/psql -At -U postgres watcher -c "select severe[$index] from arc_cid where epochtime between '$prevrun' and '$lastrun' and cid=$cid"`;
			chomp($severe1);
			if ( $severe1 eq "" )
			{
				$severe1 = 0;
			}

			$solrej1 += `/usr/bin/psql -At -U postgres watcher -c "select solrej[$index] from arc_cid where epochtime between '$prevrun' and '$lastrun' and cid=$cid"`;
			chomp($solrej1);
			if ( $solrej1 eq "" )
			{
				$solrej1 = 0;
			}

		}

		foreach $index (@evenindexes)
		{
			$solrcount2 += `/usr/bin/psql -At -U postgres watcher -c "select solrcnt[$index] from arc_cid where epochtime between '$prevrun' and '$lastrun' and cid=$cid"`;
			chomp($solrcount2);
			if ( $solrcount2 eq "" )
			{
				$solrcount2 = 0;
			}

			$sizemb2 += `/usr/bin/psql -At -U postgres watcher -c "select sizemb[$index] from arc_cid where epochtime between '$prevrun' and '$lastrun' and cid=$cid"`;
			chomp($sizemb2);
			if ( $sizemb2 eq "" )
			{
				$sizemb2 = 0;
			}

			$severe2 += `/usr/bin/psql -At -U postgres watcher -c "select severe[$index] from arc_cid where epochtime between '$prevrun' and '$lastrun' and cid=$cid"`;
			chomp($severe2);
			if ( $severe2 eq "" )
			{
				$severe2 = 0;
			}

			$solrej2 += `/usr/bin/psql -At -U postgres watcher -c "select solrej[$index] from arc_cid where epochtime between '$prevrun' and '$lastrun' and cid=$cid"`;
			chomp($solrej2);
			if ( $solrej2 eq "" )
			{
				$solrej2 = 0;
			}

		}
	}

	chomp($solrcount1);
	$Tsolrcount1 += $solrcount1;
	chomp($Tsolrcount1);

	chomp($solrcount2);
	$Tsolrcount2 += $solrcount2;
	chomp($Tsolrcount2);

	chomp($sizemb1);
	$Tsizemb1 += $sizemb1;
	chomp($Tsizemb1);

	chomp($sizemb2);
	$Tsizemb2 += $sizemb2;
	chomp($Tsizemb2);

	chomp($severe1);
	$Tsevere1 += $severe1;
	chomp($Tsevere1);

	chomp($severe2);
	$Tsevere2 += $severe2;
	chomp($Tsevere2);

	chomp($solrej1);
	$Tsolrej1 += $solrej1;
	chomp($Tsolrej1);

	chomp($solrej2);
	$Tsolrej2 += $solrej2;
	chomp($Tsolrej2);

	$mscount=`/usr/bin/psql -At -U postgres watcher -c "select sum(mscnt) from arc_cid where epochtime between '$prevrun' and '$lastrun' and fgroupid = '$groupid'"`;
	chomp($mscount);
	if ( $mscount eq "" )
	{
		$mscount = 0;
	}
	chomp($mscount);
	$Tmscount += $mscount;
	chomp($Tmscount);

	$usercnt=`/usr/bin/psql -At -U postgres watcher -c "select sum(usrcnt) from arc_cid where epochtime between '$prevrun' and '$lastrun' and fgroupid = '$groupid'"`;
	chomp($usercnt);
	if ( $usercnt eq "" )
	{
		$usercnt = 0;
	}
	chomp($usercnt);
	$Tusercnt += $usercnt;
	chomp($Tusercnt);

	$ndxbacklg=`/usr/bin/psql -At -U postgres watcher -c "select sum(solrndxbklg) from arc_cid where epochtime between '$prevrun' and '$lastrun' and fgroupid = '$groupid'"`;
	chomp($ndxbacklg);
	if ( $ndxbacklg eq "" )
	{
		$ndxbacklg = 0;
	}
	chomp($ndxbacklg);
	$Tndxbacklg += $ndxbacklg;
	chomp($Tndxbacklg);

	$solringestperday=`/usr/bin/psql -At -U postgres watcher -c "select sum(ingest24) from arc_cid where epochtime between '$prevrun' and '$lastrun' and fgroupid = '$groupid'"`;
	chomp($solringestperday);
	if ( $solringestperday eq "" )
	{
		$solringestperday = 0;
	}
	chomp($solringestperday);
	$Tsolringestperday += $solringestperday;
	chomp($Tsolringestperday);

	$indexperday=`/usr/bin/psql -At -U postgres watcher -c "select sum(ndx24) from arc_cid where epochtime between '$prevrun' and '$lastrun' and fgroupid = '$groupid'"`;
	chomp($indexperday);
	if ( $indexperday eq "" )
	{
		$indexperday = 0;
	}
	chomp($indexperday);
	$Tindexperday += $indexperday;
	chomp($Tindexperday);

	$masrej=`/usr/bin/psql -At -U postgres watcher -c "select sum(masrej) from arc_cid where epochtime between '$prevrun' and '$lastrun' and fgroupid = '$groupid'"`;
	chomp($masrej);
	if ( $masrej eq "" )
	{
		$masrej = 0;
	}
	chomp($masrej);
	$Tmasrej += $masrej;
	chomp($Tmasrej);

	if(($flags{'debug'}) || ($flags{'verbose'}))
	{
		print "\nsolr group ID\n";
		print Dumper $groupid ;
		print "\nnumber of CID front shards\n";
		print Dumper $numfront;
		print "\nnumber of CID back shards\n";
		print Dumper $numback;
		print "\nsolr1 count\n";
		print Dumper $solrcount1;
		print "\nsolr2 count\n";
		print Dumper $solrcount2;
		print "\ndisk1 size of segment\n";
		print Dumper $sizemb1;
		print "\ndisk2 size of segment\n";
		print Dumper $sizemb2;
		print "\nmailsource cnt\n";
		print Dumper $mscount;
		print "\nuser count of segment\n";
		print Dumper $usercnt;
		print "\nindex backlog\n";
		print Dumper $ndxbacklg;
		print "\nsolr1 severe errors\n";
 		print Dumper $severe1 ;
		print "\nsolr2 severe errors\n";
 		print Dumper $severe2  ;
		print "\nsolr ingest per day\n";
 		print Dumper $solringestperday ;
		print "\nsolr index per day\n";
 		print Dumper $indexperday ;
		print "\nsolr1 rejects \n";
 		print Dumper $solrej1 ;
		print "\nsolr2 rejects \n";
 		print Dumper $solrej2 ;
		print "\nmas rejects \n";
 		print Dumper $masrej ;
	}

	printf " %6d       %6d / %-6d  %10d / %-10d  %10d / %-10d    %8d    %12d    %12d        %12d    %12d    %8d / %-8d  %8d      %8d / %-8d\n", $groupid, $numfront, $numback, $solrcount1, $solrcount2, $sizemb1, $sizemb2, $mscount, $usercnt, $ndxbacklg, $solringestperday, $indexperday, $solrej1, $solrej2, $masrej, $severe1, $severe2 ;

}

printf "\n Totals       %6d / %-6d  %10d / %-10d  %10d / %-10d    %8d    %12d    %12d        %12d    %12d    %8d / %-8d  %8d      %8d / %-8d\n", $Tnumfront, $Tnumback, $Tsolrcount1, $Tsolrcount2, $Tsizemb1, $Tsizemb2, $Tmscount, $Tusercnt, $Tndxbacklg, $Tsolringestperday, $Tindexperday, $Tsolrej1, $Tsolrej2, $Tmasrej, $Tsevere1, $Tsevere2 ;


##
## Begin section 2 - server summary
##
#
# Remove the duplicates from the list of server IPs
#
undef %seen;
@serverips = grep(!$seen{$_}++, @serverips);
chomp(@serverips);

#
# Print out the headings
#
printf "\n\n\n\n      Server IP          Location      DNS count   Directory count     Free disk (MB) \n\n" ;

#
# Get the data and print out
#
foreach $ip (@serverips) 
{

	$srvtype=`/usr/bin/psql -At -U postgres watcher -c "select srv_type from arc_server where srv_ip='$ip' order by epochtime desc limit 1"`;
	chomp($srvtype);

	if ( $srvtype eq "solr" )
	{
		$location=`/usr/bin/psql -At -U postgres watcher -c "select datacenter from arc_server where srv_ip='$ip' order by epochtime desc limit 1"`;
		chomp($location);

		$dnsdircnt=`/usr/bin/psql -At -U postgres watcher -c "select dnscnt from arc_server where srv_ip='$ip' order by epochtime desc limit 1"`;
		chomp($dnsdircnt);

		$dskdircnt=`/usr/bin/psql -At -U postgres watcher -c "select dircnt from arc_server where srv_ip='$ip' order by epochtime desc limit 1"`;
		chomp($dskdircnt);

		$dskfree=`/usr/bin/psql -At -U postgres watcher -c "select diskfree from arc_server where srv_ip='$ip' order by epochtime desc limit 1"`;
		chomp($dskfree);


		printf " %15s  %15s  %10d    %10d       %12d\n", $ip, $location, $dnsdircnt, $dskdircnt, $dskfree ;

	}

}

}  #For support report end

if ( ! $flags{'summ'} )
{
##
## Begin section 3 - detailed solr data by CID
##
if ( $flags{'support'} )
{
	printf "<br><br>";
}
else
{
	printf "\n\n" ;
}

#
# Get the data and print out
#

	###foreach $groupid ("solr-300.solr.region-0.pod2.director.mxlogic.net.") 
	foreach $groupid (@groupids) 
	{
#
# Print out the headings
#
		if ( $flags{'support'} )
		{
			printf "<br><br>--------------------------- %6d ---------------------------<br><br>", $groupid ;
		}
		else
		{
			printf "\n\n--------------------------- %6d ---------------------------\n\n", $groupid ;
		}


		if ( $flags{'support'} )
		{
			printf "<br><pre>     CID      Customer Name                           solr count           solr rejects      mas rejects  Index backlog   MS cnt        MS ID        MS Backlog           MS Status                    Ingest/day </pre><br><br>" ;
		}
		else
		{
			printf "     CID      Customer Name                shard type              solr count           solr segments     solr rejects      mas rejects  disk size (MB)       Index backlog   Index/day  User count MS cnt        MS ID        MS Backlog           MS Status                    Ingest/day \n\n" ;
		}

#
# Get the list of all CIDs for this solr segment
#
		@allcids=`/usr/bin/psql -At -U postgres watcher -c "select cid from (select cid,solrcnt[1] from arc_cid where '$groupid'=any(groupid) and epochtime between '$prevrun' and '$lastrun' order by solrcnt[1] desc ) as cid_sorted"`;

		undef %seen;
		@allcids = grep(!$seen{$_}++, @allcids);
		chomp(@allcids);

		foreach $cid (@allcids)
		{
			$custnm=`/usr/bin/psql -At -U postgres watcher -c "select cstnam from arc_cid where cid='$cid' and epochtime between '$prevrun' and '$lastrun' order by epochtime desc limit 1"`;
			chomp($custnm);

			if ( `/usr/bin/psql -At -U postgres watcher -c "select fgroupid from arc_cid where cid='$cid' and epochtime between '$prevrun' and '$lastrun' order by epochtime desc limit 1"` == $groupid )
			{
				$shardtype="frontier";
			}
			else
			{
				$shardtype="back";
			}

# 
# Get the pair of indexes for this CID and Group ID to get the proper value for these 4 pair of values
#
			$solrcnt1 = 0;
			$solrcnt2 = 0;
			$solrseg1 = 0;
			$solrseg2 = 0;
			$solrej1 = 0;
			$solrej2 = 0;
			$sizemb1 = 0;
			$sizemb2 = 0;

			@cgroupids = `/usr/bin/psql -At -U postgres watcher -c "select groupid from arc_cid where epochtime between '$prevrun' and '$lastrun' and cid = $cid" | /bin/sed s/{//g | /bin/sed s/}//g | /bin/sed s/,/\\\\n/g`;
			chomp(@cgroupids);

			if($flags{'debug'})
			{
				print "\nCID = $cid\n";
				print "\nGroupID = $groupid\n";
				print "\ncgroupids\n";
				print Dumper \@cgroupids;
			}

			@oddindexes = ();
			@evenindexes = ();

			foreach $index (1..scalar(@cgroupids))
			{
				$aindex = $index - 1;
				if ( $cgroupids[$aindex] == $groupid )
				{
					push(@oddindexes, ((2*$index)-1));	
					push(@evenindexes, (2*$index));	
				}
			}

			if($flags{'debug'})
			{
				print "\nODD indexes \n";
				print Dumper \@oddindexes;
				print "\nEVEN indexes \n";
				print Dumper \@evenindexes;
			}

			foreach $index (@oddindexes)
			{
				$solrcnt1 += `/usr/bin/psql -At -U postgres watcher -c "select solrcnt[$index] from arc_cid where epochtime between '$prevrun' and '$lastrun' and cid=$cid"`;

				$solrseg1 += `/usr/bin/psql -At -U postgres watcher -c "select solrseg[$index] from arc_cid where epochtime between '$prevrun' and '$lastrun' and cid=$cid"`;

				$solrej1 += `/usr/bin/psql -At -U postgres watcher -c "select solrej[$index] from arc_cid where epochtime between '$prevrun' and '$lastrun' and cid=$cid"`;

				$sizemb1 += `/usr/bin/psql -At -U postgres watcher -c "select sizemb[$index] from arc_cid where epochtime between '$prevrun' and '$lastrun' and cid=$cid"`;

			}

			foreach $index (@evenindexes)
			{
				$solrcnt2 += `/usr/bin/psql -At -U postgres watcher -c "select solrcnt[$index] from arc_cid where epochtime between '$prevrun' and '$lastrun' and cid=$cid"`;

				$solrseg2 += `/usr/bin/psql -At -U postgres watcher -c "select solrseg[$index] from arc_cid where epochtime between '$prevrun' and '$lastrun' and cid=$cid"`;

				$solrej2 += `/usr/bin/psql -At -U postgres watcher -c "select solrej[$index] from arc_cid where epochtime between '$prevrun' and '$lastrun' and cid=$cid"`;

				$sizemb2 += `/usr/bin/psql -At -U postgres watcher -c "select sizemb[$index] from arc_cid where epochtime between '$prevrun' and '$lastrun' and cid=$cid"`;

			}

			chomp($solrcnt1);
			chomp($solrcnt2);
			chomp($solrseg1);
			chomp($solrseg2);
			chomp($solrej1);
			chomp($solrej2);
			chomp($sizemb1);
			chomp($sizemb2);

			$masrej=`/usr/bin/psql -At -U postgres watcher -c "select masrej from arc_cid where epochtime between '$prevrun' and '$lastrun' and cid='$cid' order by epochtime desc limit 1"`;
			chomp($masrej);

			$ndxbacklg=`/usr/bin/psql -At -U postgres watcher -c "select solrndxbklg from arc_cid where epochtime between '$prevrun' and '$lastrun' and cid='$cid' order by epochtime desc limit 1"`;
			chomp($ndxbacklg);

			$indexperday=`/usr/bin/psql -At -U postgres watcher -c "select ndx24 from arc_cid where epochtime between '$prevrun' and '$lastrun' and cid='$cid' order by epochtime desc limit 1"`;
			chomp($indexperday);

			$usrcnt=`/usr/bin/psql -At -U postgres watcher -c "select usrcnt from arc_cid where epochtime between '$prevrun' and '$lastrun' and cid='$cid' order by epochtime desc limit 1"`;
			chomp($usrcnt);

			$mscnt=`/usr/bin/psql -At -U postgres watcher -c "select mscnt from arc_cid where epochtime between '$prevrun' and '$lastrun' and cid='$cid' order by epochtime desc limit 1"`;
			chomp($mscnt);

			@sids=`/usr/bin/psql -At -U postgres watcher -c "select distinct(sid) from arc_mailsource where ms_epochtime between '$prevrun' and '$lastrun' and cid='$cid' and ms_epochtime between '$prevrun' and '$lastrun'"`;
			chomp(@sids);

#
# For back shards do not print out mail source information or solr data
#
			if ( $shardtype eq "back" )
			{
				@sids = ();
			}

			if ( $#sids > 0 )
			{ # more than 1 sid reported
				if ( $flags{'support'} )
				{
					printf "<pre><A HREF=\"https://%s/cs/arc_data/%d\">%12d</A>  %-30s   %12d/%-12d   %6d/%-6d      %6d        %8d      %4d  ", $hostname, $cid, $cid, (substr $custnm,0,30), $solrcnt1, $solrcnt2, $solrej1, $solrej2, $masrej, $ndxbacklg, $mscnt ;
				}
				else
				{
					printf "%12d  %-30s %-8s      %12d/%-12d   %6d/%-6d     %6d/%-6d      %6d  %11d/%-11d  %8d    %12d    %6d    %4d  ", $cid, (substr $custnm,0,30), $shardtype, $solrcnt1, $solrcnt2, $solrseg1, $solrseg2, $solrej1, $solrej2, $masrej, $sizemb1, $sizemb2, $ndxbacklg, $indexperday, $usrcnt, $mscnt ;
				}

				$count = 0;

				foreach $sid (@sids)
				{

					$msbacklg=`/usr/bin/psql -At -U postgres watcher -c "select ms_backlog from arc_mailsource where sid=$sid and ms_epochtime between '$prevrun' and '$lastrun' order by ms_epochtime desc limit 1"`;
					chomp($msbacklg);

					if ( ( $msbacklg == 1111111111 ) || ( $msbacklg == 5555555555 ) )
					{
						$msbacklg = "--UNKNOWN--";
						chomp($msbacklg);
					}

					$msstatus=`/usr/bin/psql -At -U postgres watcher -c "select ms_status from arc_mailsource where sid=$sid and ms_epochtime between '$prevrun' and '$lastrun' order by ms_epochtime desc limit 1"`;
					chomp($msstatus);

					switch ( $msstatus )
					{
						case 0  { $mstextstat = " 0 - Normal                "; }
						case 1  { $mstextstat = " 1 - No Mail               "; }
						case 2  { $mstextstat = " 2 - No Socket             "; }
						case 3  { $mstextstat = " 3 - Bad ID/Password       "; }
						case 4  { $mstextstat = " 4 - Protocol Error        "; }
						case 5  { $mstextstat = " 5 - fetchmail syntax error"; }
						case 6  { $mstextstat = " 6 - Bad Permissions       "; }
						case 7  { $mstextstat = " 7 - Timeout               "; }
						case 8  { $mstextstat = " 8 - Exclusion Error       "; }
						case 9  { $mstextstat = " 9 - Lock Busy             "; }
						case 10 { $mstextstat = "10 - Port Open Error       "; }
						case 11 { $mstextstat = "11 - DNS Error             "; }
						case 12 { $mstextstat = "12 - BSTMP Error           "; }
						case 13 { $mstextstat = "13 - fetchmail limit       "; }
						case 14 { $mstextstat = "14 - Server Busy           "; }
						case 23 { $mstextstat = "23 - Internal Error        "; }
						else    { $mstextstat = "-----UNKNOWN               "; }
					}

					$ingestperday=`/usr/bin/psql -At -U postgres watcher -c "select ms_ingest24 from arc_mailsource where sid=$sid and ms_epochtime between '$prevrun' and '$lastrun' order by ms_epochtime desc limit 1"`;
					chomp($ingestperday);

					if ( $count > 0 )
					{
						if ( $flags{'support'} )
						{
							printf "<pre>                                                                                                                                 %14d    %12s        %27s   %12d </pre>", $sid, $msbacklg, $mstextstat, $ingestperday ;
						}
						else
						{
							printf "                                                                                                                                                                                                          %14d    %12s        %27s   %12d \n", $sid, $msbacklg, $mstextstat, $ingestperday ;
						}
					}
					else
					{
						if ( $flags{'support'} )
						{
							printf " %14d    %12s        %27s   %12d </pre>", $sid, $msbacklg, $mstextstat, $ingestperday ;
						}
						else
						{
							printf " %14d    %12s        %27s   %12d \n", $sid, $msbacklg, $mstextstat, $ingestperday ;
						}
					}

					$count++;

				}
			}
			else # mscnt is 0 or 1
			{
				if ( $#sids < 0 )
				{ # no sids reported
					if ( $shardtype eq "back" )
					{
						if ( $flags{'support'} )
						{
							printf "<pre><A HREF=\"https://%s/cs/arc_data/%d\">%12d</A>  %-30s   %12d/%-12d  </pre>", $hostname, $cid, $cid, (substr $custnm,0,30), $solrcnt1, $solrcnt2 ;
						}
						else
						{
							printf "%12d  %-30s %-8s      %12d/%-12d   %6d/%-6d                                %11d/%-11d \n", $cid, (substr $custnm,0,30), $shardtype, $solrcnt1, $solrcnt2, $solrseg1, $solrseg2, $sizemb1, $sizemb2 ;
						}
					}
					else
					{
						if ( $flags{'support'} )
						{
							printf "<pre><A HREF=\"https://%s/cs/arc_data/%d\">%12d</A>  %-30s   %12d/%-12d   %6d/%-6d      %6d        %8d      %4d </pre>", $hostname, $cid, $cid, (substr $custnm,0,30), $solrcnt1, $solrcnt2, $solrej1, $solrej2, $masrej, $ndxbacklg, $mscnt ;
						}
						else
						{
							printf "%12d  %-30s %-8s       %12d/%-12d   %6d/%-6d     %6d/%-6d      %6d  %11d/%-11d  %8d    %12d    %6d    %4d \n", $cid, (substr $custnm,0,30), $shardtype, $solrcnt1, $solrcnt2, $solrseg1, $solrseg2, $solrej1, $solrej2, $masrej, $sizemb1, $sizemb2, $ndxbacklg, $indexperday, $usrcnt, $mscnt ;
						}
					}
				}
				else
				{ # only 1 sid reported

					$sid = $sids[0];
           		chomp($sid);

					$msbacklg=`/usr/bin/psql -At -U postgres watcher -c "select sum(ms_backlog) from (select ms_backlog from arc_mailsource where cid='$cid' and ms_epochtime between '$prevrun' and '$lastrun' order by ms_epochtime desc limit $mscnt ) as backlog_tot"`;
					chomp($msbacklg);

					if ( ( $msbacklg == 1111111111 ) || ( $msbacklg == 5555555555 ) )
					{
						$msbacklg = "--UNKNOWN--";
						chomp($msbacklg);
					}

					$msstatus=`/usr/bin/psql -At -U postgres watcher -c "select ms_status from arc_mailsource where sid=$sid and ms_epochtime between '$prevrun' and '$lastrun' order by ms_epochtime desc limit 1"`;
					chomp($msstatus);

					switch ( $msstatus )
					{
						case 0  { $mstextstat = " 0 - Normal                "; }
						case 1  { $mstextstat = " 1 - No Mail               "; }
						case 2  { $mstextstat = " 2 - No Socket             "; }
						case 3  { $mstextstat = " 3 - Bad ID/Password       "; }
						case 4  { $mstextstat = " 4 - Protocol Error        "; }
						case 5  { $mstextstat = " 5 - fetchmail syntax error"; }
						case 6  { $mstextstat = " 6 - Bad Permissions       "; }
						case 7  { $mstextstat = " 7 - Timeout               "; }
						case 8  { $mstextstat = " 8 - Exclusion Error       "; }
						case 9  { $mstextstat = " 9 - Lock Busy             "; }
						case 10 { $mstextstat = "10 - Port Open Error       "; }
						case 11 { $mstextstat = "11 - DNS Error             "; }
						case 12 { $mstextstat = "12 - BSTMP Error           "; }
						case 13 { $mstextstat = "13 - fetchmail limit       "; }
						case 14 { $mstextstat = "14 - Server Busy           "; }
						case 23 { $mstextstat = "23 - Internal Error        "; }
						else    { $mstextstat = "-----UNKNOWN               "; }
					}

					$ingestperday=`/usr/bin/psql -At -U postgres watcher -c "select sum(ms_ingest24) from (select ms_ingest24 from arc_mailsource where cid='$cid' and ms_epochtime between '$prevrun' and '$lastrun' order by ms_epochtime desc limit $mscnt ) as ingest24_tot"`;
					chomp($ingestperday);

					if ( $flags{'support'} )
					{
						printf "<pre><A HREF=\"https://%s/cs/arc_data/%d\">%12d</A>  %-30s   %12d/%-12d   %6d/%-6d      %6d        %8d      %4d   %14d    %12s        %27s   %12d </pre>", $hostname, $cid, $cid, (substr $custnm,0,30), $solrcnt1, $solrcnt2, $solrej1, $solrej2, $masrej, $ndxbacklg, $mscnt, $sid, $msbacklg, $mstextstat, $ingestperday ;
					}
					else
					{
						printf "%12d  %-30s %-8s      %12d/%-12d   %6d/%-6d     %6d/%-6d      %6d  %11d/%-11d  %8d    %12d    %6d    %4d   %14d    %12s        %27s   %12d \n", $cid, (substr $custnm,0,30), $shardtype, $solrcnt1, $solrcnt2, $solrseg1, $solrseg2, $solrej1, $solrej2, $masrej, $sizemb1, $sizemb2, $ndxbacklg, $indexperday, $usrcnt, $mscnt, $sid, $msbacklg, $mstextstat, $ingestperday ;
					}

				}

			}

		}

	}

}

if ( $flags{'support'} )
{
	printf "</HTML>";
	printf "</BODY>";
}
else
{
	printf "\n\n" ;
}

##
## Get all command line options into flags hash
##
sub get_opts() 
{

	use Getopt::Long qw(:config no_ignore_case bundling);
	Getopt::Long::Configure("bundling");
	GetOptions
	(
		'summ|s'        => \$flags{'summ'},
		'support|p'     => \$flags{'support'},
		'debug|d'       => \$flags{'debug'},
		'verbose|v'     => \$flags{'verbose'},

		'help|usage|h'  => sub {warn &usage; exit 1;}
	) or die &usage;

	if ( defined($flags{'summ'}) && $flags{'support'} )
	{
		print "\nERROR: --summ and --support are mutually exclusive.\n\n";
		usage;
		exit 2;
	}

	defined($flags{'summ'}) || ($flags{'summ'} = 0);
	defined($flags{'support'}) || ($flags{'support'} = 0);
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
    -s, --summ                Summary data only (No detailed CID report - section 3)
    -p, --support             Support data for web (No summary and limited detailed data)
    -d, --debug               Debug Mode (limited CIDs no DB writes)
    -h, --help                Print this help
EOF

        print $usage;
}

