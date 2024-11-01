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
use Storable;

our $AUTHOR     = 'John_Vossler@McAfee.com';
our $VERSION    = 2.0;
our $PODCONFIG  = '/mxl/etc/pod_config.xml';

our %flags = ();

our $start = ();
our $end = ();
our $iteration = ();

$ENV{'PGPASSWORD'} = 'dbG0d';    # Export the postgres password

sub get_opts();
sub usage();
sub main();

&get_opts();

if($flags{'debug'})
{
	print "flags\n";
	print Dumper(\%flags);
}

##
## Global variables
##
my $pair_dist = ();
my @pair_dist = ();
my $region = ();
my $region_pair = ();
my $pair = ();
my $config = ("/usr/local/bin/opsadmin/watcher/watcher_archive.xml");
my $dbname = ("watcher");

##
## Create variables for CID processing
##
#
# Create the hashes for CIDs
#
my %timestamp = ();              # epoch seconds
my $severe = ();
my $tsize = ();
my $solrsegtmp = ();
my $solrcnt = ();
my $scnt = ();
my $tscnt = ();
my %solrips = ();
my %solrcnt = ();
my %solrseg = ();
my %solrindexcnt = ();
my %solrej = ();
my %solrsizemb = ();
my %solrsevere = ();
my %cstnam = ();
my %spurge = ();
my %rpurge = ();
my %fgroupid = ();
my %groupid = ();
my @groupids = ();
my $solrseg = ();
my $solrindexcnt = ();
my $solrej = ();
my $sizemb = ();
my %masrej = ();
my %mscnt  = ();
my %usrcnt = ();
my %solrndxbklg = ();
my %solrseqnum = ();
my %ndx24 = ();
my %ingest24 = ();
my %segment = ();

#
# Non hashes
#
my $policydb = ();
my $archivedb = ();
my $shard = ();
my @shards = ();
my $shardname = ();
my @shardnames = ();
my $solrndxseqn = ();
my $count = ();
my $results = ();
my $cid_counter = ();
my @cids = ();
my $ip = ();
my @ipsolr = ();
my @ndx = ();
my @solr = ();
my $indexstr = ();
my @indexcnt = ();
my @mailers = ();
my @sqlmailers = ();
my @arrayips = ();
my @arraycnt = ();
my @arrayseg = ();
my @arrayindex = ();
my @arrayrej = ();
my @arraysizemb = ();
my @arraysevere = ();
my @arraygroupid = ();
my $ms_ref = ();
my %ms_hash = ();
my $cmd = ();
my $ndxseqstr = ();
my @ndxseq = ();
my $insert = ();
my $dbh = ();
my $sth = ();
my $cid = ();
my $xp = ();
my $array = ();
my $ip_list = ();
my @ingestmaspool = ();
my @searchmaspool = ();
my @maspool = ();
my @size = ();
my @solrips = ();
my @solrips0 = ();
my @solrips1 = ();
my @solrips2 = ();

#
# Create the server specific hashes
#
my %srv_timestamp = ();
my %srv_type = ();
my %datacenter = ();
my %dircnt = ();
my %diskfree = ();

#
# Non hashes for servers
#
my @masips = ();
my @ips = ();

#
# Define Storage variables
#
my %nfs_timestamp = ();

my $ts = ();
my $cidseqnum = ();
my %seen = ();
my $index = ();

##
## Get the global information needed
##
#
# Get IP of policy database server
#
	$policydb = `/mxl/bin/get_config_entry -f $PODCONFIG -S "pod/db?path"`;
	chomp($policydb);

	if(($flags{'debug'}) || ($flags{'verbose'}))
	{
		print "\npolicy DB IP address\n";
		print Dumper $policydb;
	}

#
# Get region number
#
   $region = `/mxl/sbin/arregion`;
   chomp($region);

   if(($flags{'debug'}) || ($flags{'verbose'}))
   {
      print "\nRegion number for pair\n";
      print Dumper $region;
   }

#
# Get a list of mas and solr servers from both sites
#
   @masips = @{retrieve("/tmp/watcher_masIPs")};

   @solrips = @{retrieve("/tmp/watcher_solrIPs")};

#
# Get a list of solr servers from both sites
#
   %srv_type = %{retrieve("/tmp/watcher_SRVtype")};

   %datacenter = %{retrieve("/tmp/watcher_DCs")};

	if (( $flags{'debug'} ) || ( $flags{'verbose'} ))
	{
		print "\nmas ip addresses\n";
		print Dumper \@masips;
		print "\nsolr ip addresses\n";
		print Dumper \@solrips;
		print "\nmas ip addresses\n";
		print Dumper \@masips;
		print "\ndata center\n";
		print Dumper \%datacenter;
		print "\nserver type\n";
		print Dumper \%srv_type;
	}
	
#
# Get IP of archiving database server
#
	$archivedb = `/mxl/bin/get_config_entry -f $PODCONFIG -S "pod/arc_db?path"`;
	chomp($archivedb);

	if(($flags{'debug'}) || ($flags{'verbose'}))
	{
		print "\narchive DB IP address\n";
		print Dumper $archivedb;
	}

#
# Get the list of CIDs
#
   @cids = @{retrieve("/tmp/watcher_CIDs")};

   if(($flags{'debug'}) || ($flags{'verbose'}))
   {
      print "\ncids\n";
      print Dumper \@cids;
      print Dumper $#cids;
   }

	$ts = localtime(time());

	print "###################################################################################### \n";
	print "\n";
	print "Starting new instance at ";
	print Dumper $ts;
	print "\n";
	print "\nStart index = $start   Ending index = $end \n";
	print "\n";
	print "###################################################################################### \n";

	$cid_counter = 0;
##
## Get data and populate the CID table
##
   foreach $index ($start..$end)
   {
      $cid = $cids[$index];

		print "\n\nWorking CID: getting data, $start  $index  $end\n";
		print Dumper $cid;

		$cid_counter++;

		$timestamp{$cid} = time();

		if (( $flags{'debug'} ) || ( $flags{'verbose'} ))
		{
			print "\ntimestamp\n";
			print Dumper \%timestamp;
		}

#
# Get the number of purged messages for this CID
#   selective purge
#   retention purge
#
		$spurge{$cid} = `/usr/bin/psql -At -h $archivedb -U postgres mxl_archive -c "select sum(message_count) from arc_spurge_job where customer_id=$cid"`;
		chomp($spurge{$cid});

		if ( $spurge{$cid} eq "" )
		{
			$spurge{$cid} = 0;
			chomp($spurge{$cid});
		}

		$rpurge{$cid} = `/usr/bin/psql -At -h $archivedb -U postgres mxl_archive -c "select count(*) from mxl_arc_message_purge_log where customer_id=$cid"`;
		chomp($rpurge{$cid});

		if ( $rpurge{$cid} eq "" )
		{
			$rpurge{$cid} = 0;
			chomp($rpurge{$cid});
		}

		if (( $flags{'debug'} ) || ( $flags{'verbose'} ))
		{
			print "\nSelective purge\n";
			print Dumper \%spurge;
			print "\nRetention purge\n";
			print Dumper \%rpurge;
		}

#
# Get IPs of ingest and search mas pools and solr ips for this CID
#
		@ipsolr = `/mxl/sbin/solr --region $region --cust $cid shards | /bin/awk ' \$1 == cid {print \$6}' cid=$cid | /bin/awk -F\: '{print \$1}' | /bin/awk '!x[\$0]++'`;
		chomp(@ipsolr);

		if ( $#ipsolr >= 0 )
		{
			push @{$solrips{$cid}}, @ipsolr;
			chomp($solrips{$cid});
		}

		@ingestmaspool = (split(/\s+/,`/mxl/sbin/dnsdir-cust $cid ingest.mas | grep -v undefined | sed s/","/" "/g`));
		chomp(@ingestmaspool);

		@searchmaspool = (split(/\s+/,`/mxl/sbin/dnsdir-cust $cid search.mas | grep -v undefined | sed s/","/" "/g`));
		chomp(@searchmaspool);

		if (( $flags{'debug'} ) || ( $flags{'verbose'} ))
		{
			print "\nsolr IPs array\n";
			print Dumper \@ipsolr;
			print "\nsolr IPs Hash Array\n";
			print Dumper \%solrips;
			print "\ningest mas IPs\n";
			print Dumper \@ingestmaspool;
			print "\nsearch mas IPs\n";
			print Dumper \@searchmaspool;
		}

#
# Get the sum of ingest index backlogs over the ingest mas pool
#
		$solrndxbklg{$cid} = 0;

		foreach $ip (@ingestmaspool)
		{
			$solrndxbklg{$cid} += `ssh $ip "su - mxl-archive -c '/usr/bin/find /var/tmp/index_queue/CID-$cid/ -type f -print'" 2>/dev/null | wc -l`;
			chomp($solrndxbklg{$cid});
		}

		if (( $flags{'debug'} ) || ( $flags{'verbose'} ))
		{
			print "\nsolr index backlog over mas pool\n";
			print Dumper \%solrndxbklg;
		}

#
# Get Customer name text
#
		$cstnam{$cid} = `/usr/bin/psql -At -h $policydb -U postgres mxl -c "select name from mxl_customer where customer_id=$cid"`;
		chomp($cstnam{$cid});

		if ( $cstnam{$cid} eq "" )
		{
			$cstnam{$cid} = "unknown-UNKNOWN-unknown-UNKNOWN-no mxl_customer entry";
		}

		if (( $flags{'debug'} ) || ( $flags{'verbose'} ))
		{
			print "\ncustomer name\n";
			print Dumper \%cstnam;
		}

#
# Get the groupid for this CIDs frontier shard
#
		$fgroupid{$cid} = `/mxl/sbin/solr --region $region --cust $cid shards | /bin/awk '\$3 == 1 {print \$4}' | /usr/bin/tail -1`;

		chomp($fgroupid{$cid});

		if (( $flags{'debug'} ) || ( $flags{'verbose'} ))
		{
			print "\nfrontier groupid from core manager\n";
			print Dumper \%fgroupid;
		}

#
# Get the groupid for all shards for this CID iff fgroupid is not empty
#
		if (( $fgroupid{$cid} eq "" ) || ( $fgroupid{$cid} !~ /^[+-]?\d+$/ ))
		{
			foreach $ip (@ipsolr)
			{

				push @{$groupid{$cid}}, 0;
				chomp($groupid{$cid});

			}
		}
		else
		{

			@groupids = `/mxl/sbin/solr --region $region --cust $cid shards | /bin/awk '\$1 == cid {print \$4}' cid=$cid | /bin/awk '!x[\$0]++'`;
			chomp(@groupids);

			push @{$groupid{$cid}}, @groupids;
			chomp($groupid{$cid});
		}

		if ( $#{$groupid{$cid}} < 0 )
		{
			push @{$groupid{$cid}}, 0;
		}

		chomp($groupid{$cid});

		if (( $flags{'debug'} ) || ( $flags{'verbose'} ))
		{
			print "\nAll groupids from core manager\n";
			print Dumper \%groupid;
		}

#
# Get the solr sequence number for this CID
#
		$solrseqnum{$cid} = `/mxl/sbin/arseqcnt $cid 2>/dev/null`;
		chomp($solrseqnum{$cid});

		if (( $flags{'debug'} ) || ( $flags{'verbose'} ))
		{
			print "\nsolr sequence number\n";
			print Dumper \%solrseqnum;
		}

##
## Get the solr core count, solr segment count and solr reject count over all the solr(s) defined for this CID
##
		foreach $ip (@ipsolr)
		{

#
# Get the solr count for this CID on this solr for all shards
#
			if ( $solrseqnum{$cid} eq "" )
			{
				push @{$solrcnt{$cid}}, 0;
				chomp($solrcnt{$cid});
			}
			else
			{
#
# Get the list of shards that are on this solr for this CID
#
				@shards = `/mxl/sbin/solr --region $region --solr $ip --cust=$cid shards | awk '\$1 == cid && \$6 ~ hip {print \$1"-"\$2}' cid=$cid hip=$ip`;
				chomp(@shards);

				$tscnt = 0;

				foreach $shard (@shards)
				{

					$scnt = "";

					$scnt = `/mxl/sbin/solr --region $region --solr $ip --cust=$cid --shard $shard count 2>/dev/null`;
					chomp($scnt);
					if (( $scnt =~ /\D/ ) || ( $scnt eq "" ))
					{
						sleep(2);
						$scnt = `/mxl/sbin/solr --region $region --solr $ip --cust=$cid --shard $shard count 2>/dev/null`;
						chomp($scnt);
						if (( $scnt =~ /\D/ ) || ( $scnt eq "" ))
						{
							$scnt = -1;
						}
					}

					$tscnt += $scnt;

				}
			}

			push @{$solrcnt{$cid}}, $tscnt;
			chomp($solrcnt{$cid});
	
			###if ( $#{$solrcnt{$cid}} < 0 )
			###{
				###push @{$solrcnt{$cid}}, 0;
			###}
			###chomp($solrcnt{$cid});

			if (( $flags{'debug'} ) || ( $flags{'verbose'} ))
			{
				print "\nsolr count array\n";
				print Dumper \%solrcnt;
			}

##
## Sum the data over multiple shards on same IP for same CID
##
			@shardnames = `/mxl/sbin/solr --region $region --cust $cid shards | /bin/awk ' \$6 ~ hip {print \$1"-"\$2}' hip=$ip`;
			chomp(@shardnames);

			$solrindexcnt = 0;
			$solrseg = 0;
			$solrej = 0;
			$tsize = 0;

			foreach $shardname (@shardnames)
			{
				$solrindexcnt += `ssh $ip "su - mxl-archive -c '/bin/ls -1 /mxl/msg_archive/solr/data/$shardname/index'" 2>/dev/null | /bin/grep -c "_"`;
				chomp($solrindexcnt);

				$solrsegtmp = `curl -m 1 -s "http://$ip:8080/solr/$shardname/admin/stats.jsp" | grep segments | /bin/awk '!x[\$0]++' | /bin/awk -F\= '{print \$5}' | sed s/}//g`;
				chomp($solrsegtmp);

				if (( $solrsegtmp eq "" ) || ( $solrsegtmp !~ /^[+-]?\d+$/ ))
				{
					$solrsegtmp = 0;
					chomp($solrsegtmp);
				}

				$solrseg += $solrsegtmp;

				@size = split(/\s+/,`ssh $ip "su - mxl-archive -c 'if [ -d /mxl/msg_archive/solr/data/$shardname ] ; then /usr/bin/du -sm /mxl/msg_archive/solr/data/$shardname ; else echo 0 ; fi'"`);

				if ( $size[0] eq "" )
				{
					$size[0] = 0;
					chomp($size[0]);
				}

				$tsize += $size[0];

			}

			push @{$solrindexcnt{$cid}}, $solrindexcnt;
			chomp($solrindexcnt{$cid});

			push @{$solrseg{$cid}}, $solrseg;
			chomp($solrseg{$cid});

			push @{$solrsizemb{$cid}}, $tsize;
			chomp($solrsizemb{$cid});

			$solrej = `ssh $ip "if [ -d /var/tmp/index_reject/$ip/$cid ] ; then /usr/bin/find /var/tmp/index_reject/*/$cid  -type f -name 111111* -print | wc -l ; else echo 0;fi"`;
			chomp($solrej);

			if ( $solrej eq "" )
			{
				$solrej = 0;
			}

			push @{$solrej{$cid}}, $solrej;
			chomp($solrej{$cid});

		}

		if ( $#{$solrindexcnt{$cid}} < 0 )
		{
			push @{$solrindexcnt{$cid}}, 0;
			chomp($solrindexcnt{$cid});
		}

		if ( $#{$solrseg{$cid}} < 0 )
		{
			push @{$solrseg{$cid}}, 0;
			chomp($solrseg{$cid});
		}

		if ( $#{$solrej{$cid}} < 0 )
		{
			push @{$solrej{$cid}}, 0;
			chomp($solrej{$cid});
		}

		if ( $#{$solrsizemb{$cid}} < 0 )
		{
			push @{$solrsizemb{$cid}}, 0;
			chomp($solrsizemb{$cid});
		}

		if (( $flags{'debug'} ) || ( $flags{'verbose'} ))
		{
			print "\nsolr index file count\n";
			print Dumper \%solrindexcnt;
			print "\nsolr segment count\n";
			print Dumper \%solrseg;
			print "\nsolr rejects\n";
			print Dumper \%solrej;
			print "\nsize on all solrs\n";
			print Dumper \%solrsizemb;
		}

#
# Get the sum of mas rejects over all the ingest and mas pools
#
     	$masrej{$cid} = 0;

		@maspool = (@ingestmaspool, @searchmaspool);

		foreach $ip (@maspool)
		{
      	$masrej{$cid} += `ssh $ip "if [ -d /var/tmp/index_reject/mas/$cid ] ; then /usr/bin/find /var/tmp/index_reject/mas/$cid  -type f -name 111111* -print | wc -l ; else echo 0;fi"`;
      	chomp($masrej{$cid});
		}

		if (( $flags{'debug'} ) || ( $flags{'verbose'} ))
		{
			print "\nmas rejects\n";
			print Dumper \%masrej;
		}

#
# Get the user count for this CID
#
		$usrcnt{$cid} = `/usr/bin/psql -At -h $policydb -U postgres mxl -c "select billed_users_qty from arc_product_settings where id=$cid"`;
		chomp($usrcnt{$cid});
		if ( $usrcnt{$cid} eq "" )
		{
			$usrcnt{$cid} = 0;
		}

		if (( $flags{'debug'} ) || ( $flags{'verbose'} ))
		{
			print "\nuser count\n";
			print Dumper \%usrcnt;
		}

		$mscnt{$cid} = `/usr/bin/psql -At -h $archivedb -U postgres mxl_archive -c "select count(server_id) from arc_mail_source where id=$cid"`;
		chomp($mscnt{$cid});
		if ( $mscnt{$cid} eq "" )
		{
			$mscnt{$cid} = 0;
		}

		if (( $flags{'debug'} ) || ( $flags{'verbose'} ))
		{
			print "\nmail source count\n";
			print Dumper \%mscnt;
		}

#
# Get number of messages indexed in last 24 hours
#
		@ndx = split(/\s+/,`/mxl/sbin/arindex-search -c $cid -q 'index_date:[NOW-1DAYS TO NOW]' -n 1 -f 'return_all' 2>&1 1>/dev/null`);
		if (( $ndx[0] eq "No" || $ndx[0] eq "Solr" ))
		{
			$ndx[1] = 0;
		}
		$ndx24{$cid} = $ndx[1];
		chomp($ndx24{$cid});
		
		if (( $flags{'debug'} ) || ( $flags{'verbose'} ))
		{
			print "\nnumber of indexed messages in last 24 hours\n";
			print Dumper \%ndx24;
		}

#
# Get number of messages ingested in last 24 hours
#
      @mailers = `/usr/bin/psql -At -h $archivedb -U postgres mxl_archive -c "select server_id from arc_mail_source where id = $cid"`;
		chomp(@mailers);

		if ( $#mailers > 0 )
		{
			@sqlmailers = join(', ', @mailers);
		}
		else
		{
			@sqlmailers = @mailers;
		}

		if (( $flags{'debug'} ) || ( $flags{'verbose'} ))
		{
			print "\nmailers\n";
			print Dumper \@mailers;
			print "\nSQL mailers\n";
			print Dumper \@sqlmailers;
		}

		if ( $#mailers < 0 )
		{
			$ingest24{$cid} = 0 ;
		}
		else
		{
			$ingest24{$cid} = `/usr/bin/psql -At -h $archivedb -U postgres mxl_archive -c "select sum(ingested_msg_count) from arc_mail_source_poll_log where ms_server_id in (@sqlmailers) and poll_stop > now() - '24 hours'::interval"`;
			chomp($ingest24{$cid});

			if ( $ingest24{$cid} eq "" )
			{
				$ingest24{$cid} = 0 ;
				chomp($ingest24{$cid});
			}
		}

		if (( $flags{'debug'} ) || ( $flags{'verbose'} ))
		{
			print "\nnumber of ingested messages in last 24 hours\n";
			print Dumper \%ingest24;
		}

#
# Get the number of sever errors for each solr
#
		foreach $ip (@ipsolr)
		{
			$severe = `ssh $ip /bin/grep -c SEVERE /var/log/mxl/tomcat/catalina.out`;
			chomp($severe);
			if ( $severe eq "" )
			{
				$severe = 0;
			}

			push @{$solrsevere{$cid}}, $severe;
			chomp($solrsevere{$cid});

		}
	
		if ( $#{$solrsevere{$cid}} < 0 )
		{
			push @{$solrsevere{$cid}}, 0;
			chomp($solrsevere{$cid});
		}

		if (( $flags{'debug'} ) || ( $flags{'verbose'} ))
		{
			print "\nnumber of severe errors for tomcat solr (cataline.out)\n";
			print Dumper \%solrsevere;
		}

	}  # end of cid loop

##
## End CID loop
##

##
## Populate the Archiving tables in the watcher database
##
$cid_counter = 0;

##
## Populate the CID table
##
   foreach $index ($start..$end)
   {
      $cid = $cids[$index];

		print "\n\nInserting CID \n";
		print Dumper $cid;

      $cid_counter++;

      $dbh = DBI->connect("dbi:Pg:dbname=$dbname;host=localhost","postgres","") || die $DBI::errstr;

      $insert = "insert into arc_cid (epochtime,cid,cstnam,fgroupid,groupid,masrej,mscnt,usrcnt,solrndxbklg,solrseqnum,ndx24,ingest24,spurge,rpurge,solrips,sizemb,solrseg,solrcnt,solrej,severe,solrindexcnt) values (to_timestamp(?),?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

#
# Put the hash arrays in proper form for postgres insertion.
#
		if ( $#{$solrips{$cid}} < 0 )
		{
			push @{$solrips{$cid}}, '0.0.0.0';
			chomp($solrips{$cid});
		}

		if (( $flags{'debug'} ) || ( $flags{'verbose'} ))
		{
			print "\nIP array = @{$solrips{$cid}} \n";
			print "\nsolr count = @{$solrcnt{$cid}} \n";
			print "\nsolr segments = @{$solrseg{$cid}} \n";
			print "\nsolr index file count = @{$solrindexcnt{$cid}} \n";
			print "\nsolr rejects = @{$solrej{$cid}} \n";
			print "\nsizemb = @{$solrsizemb{$cid}} \n";
			print "\nsevere errors = @{$solrsevere{$cid}} \n";
			print "\ngroupids = @{$groupid{$cid}} \n";
		}

		@arrayips = @{$solrips{$cid}};
		@arrayips = "{" . join(', ', @arrayips) . "}";

		@arraycnt = @{$solrcnt{$cid}};
		@arraycnt = "{" . join(', ', @arraycnt) . "}";

		@arrayindex = @{$solrindexcnt{$cid}};
		@arrayindex = "{" . join(', ', @arrayindex) . "}";

		@arrayseg = @{$solrseg{$cid}};
		@arrayseg = "{" . join(', ', @arrayseg) . "}";

		@arrayrej = @{$solrej{$cid}};
		@arrayrej = "{" . join(', ', @arrayrej) . "}";

		@arraysizemb = @{$solrsizemb{$cid}};
		@arraysizemb = "{" . join(', ', @arraysizemb) . "}";

		@arraysevere = @{$solrsevere{$cid}};
		@arraysevere = "{" . join(', ', @arraysevere) . "}";

		@arraygroupid = @{$groupid{$cid}};
		@arraygroupid = "{" . join(', ', @arraygroupid) . "}";

		if (( $flags{'debug'} ) || ( $flags{'verbose'} ))
		{
			print "\nIP array for insert = @arrayips \n";
			print "\nsolr count array for insert = @arraycnt \n";
			print "\nsolr segments array for insert = @arrayseg \n";
			print "\nsolr index file count array for insert = @arrayindex \n";
			print "\nsolr rejects array for insert = @arrayrej \n";
			print "\nsizemb array for insert = @arraysizemb \n";
			print "\nsevere errors array for insert = @arraysevere \n";
			print "\ngroupid array for insert = @arraygroupid \n";
		}

#
# Insert or report for the DB
#
      $sth = $dbh->prepare($insert);

      if ( $flags{'debug'} )
      {        # do not populate the database when debugging

         print "\ninsert command\n";
         print Dumper $insert;
         print "execute($timestamp{$cid},$cid,$cstnam{$cid},$fgroupid{$cid},@arraygroupid,$masrej{$cid},$mscnt{$cid},$usrcnt{$cid},$solrndxbklg{$cid},$solrseqnum{$cid},$ndx24{$cid},$ingest24{$cid},$spurge{$cid},$rpurge{$cid},@arrayips,@arraysizemb,@arrayseg,@arraycnt,@arrayrej,@arraysevere,@arrayindex)";
      }

      else
      {
         if ( $flags{'verbose'} )
         {
            print "\ninsert command\n";
            print Dumper $insert;
            print Dumper $dbh;
            print Dumper $sth;
         }

			print "execute($timestamp{$cid},$cid,$cstnam{$cid},$fgroupid{$cid},@arraygroupid,$masrej{$cid},$mscnt{$cid},$usrcnt{$cid},$solrndxbklg{$cid},$solrseqnum{$cid},$ndx24{$cid},$ingest24{$cid},$spurge{$cid},$rpurge{$cid},@arrayips,@arraysizemb,@arrayseg,@arraycnt,@arrayrej,@arraysevere,@arrayindex)\n";

         $sth->execute($timestamp{$cid},$cid,$cstnam{$cid},$fgroupid{$cid},@arraygroupid,$masrej{$cid},$mscnt{$cid},$usrcnt{$cid},$solrndxbklg{$cid},$solrseqnum{$cid},$ndx24{$cid},$ingest24{$cid},$spurge{$cid},$rpurge{$cid},@arrayips,@arraysizemb,@arrayseg,@arraycnt,@arrayrej,@arraysevere,@arrayindex) || die $DBI::errstr;

         $sth->finish();
         $dbh->disconnect();

      }

	}  #end write CID information loop

##
## End loop to populate CID table
##

##
## Put an entry to mark the last successful completion
##
	if ( ! $flags{'test'} )
	{
		my $lr_timestamp = time();

		$insert = "psql -U postgres -d $dbname -h localhost -c \"insert into arc_cid_lastrun (epochtime,thread) values (to_timestamp($lr_timestamp),$iteration)\"";

		if ( $flags{'debug'} ) 
		{			# do not populate the database when debugging
			print "\narc_lastrun insert command\n";
			print Dumper $insert;
		}
		else
		{
			if ( $flags{'verbose'} )
			{
				print "\narc_lastrun insert command\n";
				print Dumper $insert;
			}
			print "\narc_lastrun insert \n";
    		system("$insert");
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
	GetOptions
	(
		'debug|d'       => \$flags{'debug'},
		'test|t'       => \$flags{'test'},
		'verbose|v'     => \$flags{'verbose'},
		'start|s=i'     => \$start,
      'end|e=i'         => \$end,
      'iteration|i=i'     => \$iteration,

		'help|usage|h'  => sub {warn &usage; exit 1;}
	) or die &usage;

	defined($flags{'debug'}) || ($flags{'debug'} = 0);
	defined($flags{'verbose'}) || ($flags{'verbose'} = 0);

	defined($start) || die &usage;
   defined($end) || die &usage;
   defined($iteration) || die &usage;
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
    -t, --test                Test Mode (limited CIDs)
	 -s, --start               Start subscript
    -e, --end                 Ending subscript
    -i, --iteration           Thread number
    -h, --help                Print this help
EOF

        print $usage;
}

