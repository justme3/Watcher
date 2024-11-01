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

our $AUTHOR     = 'John_Vossler@McAfee.com';
our $VERSION    = 0.5;
our $PODCONFIG  = '/mxl/etc/pod_config.xml';

our %flags = ();

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

#################################################################################################
#################################################################################################
#################################################################################################
#################################################################################################

seat_count=`/usr/local/bin/opsadmin/seatcount.pl $policydb`

user_count=`/usr/bin/psql -At -h $policydb -U postgres mxl -c "select count(distinct(user_id)) FROM mxl_user"`


# Users deleted
users_deleted=`ssh $policydb "/usr/local/bin/opsadmin/avg_users_deleted.sh $month"`
echo $users_deleted

# Average Message Size (slow)
for database in $logdb; do
	messages_bytes=`/usr/bin/psql -h $database -U postgres mxl_log -P tuples_only -P format=unaligned -c "SELECT SUM(CASE WHEN field='Messages Total' THEN value ELSE 0 END), SUM(CASE WHEN field='Bytes Received' THEN value ELSE 0 END) FROM mxl_log_accum WHERE TO_CHAR(created,'YYYY-MM')='$month' AND domain!='system' AND domain!='bounce.mxlogic.com'"`
	let messages_total+=`echo $messages_bytes |cut -d'|' -f1`
	let bytes_total+=`echo $messages_bytes |cut -d'|' -f2`
done
avg_message_size=`echo "scale=2;($bytes_total/$messages_total)/1024" |bc`
echo "Average Message Size: $avg_message_size kilobytes"

# Average message volume per seat
message_vol_per_seat=`echo "scale=2;$messages_total/$seat_count" |bc`
echo "Average message volume per seat: $message_vol_per_seat"

# Average message volume per user
message_vol_per_user=`echo "scale=2;$messages_total/$user_count" |bc`
echo "Average message volume per user: $message_vol_per_user"

#################################################################################################
#################################################################################################
#################################################################################################
#################################################################################################

--[ EDS ]-----------------------------------------
Total Seat Count: 1831951
Total Users: 2278825
Average users deleted per week over the month of 2010-10: 20628
Average Message Size: 13.40 kilobytes
Average message volume per seat: 2366.71
Average message volume per user: 1902.60
Bandwidth Utilization (Data393), 95th percentile: Out: 419.70 Mbps, In: 433.83 Mbps
Average worker counts for the month of 2010-10 (Data393): 7.58
Average worker counts for the month of 2010-10 (Level 3):  5.95
Average connection counts for the month of 2010-10 (Data393): 117.42
Average connection counts for the month of 2010-10 (Level 3):  107.27
Time to MTA Failure (Data393): 240.00 minutes.
Time to MTA Failure (Level 3): 272.72 minutes.
SLAs for pod 1:
#################################################################################################
rtal.mxlogic.com    99.998% Uptime
- mail.inbound10.mxlogic.net    99.975% Uptime
Total # of Msg Processed (Month): 4335704395
Average inbound messages per day: 139861432.09
Average outbound messages per day: 2921654.83
Average message timing for the month of 2010-10: 877.81 ms
Average timing per module for the month of 2010-10:
 * mod_access                   39.2639 ms
 * mod_quar                     43.8554 ms
 * mod_rebuild                  12.0765 ms
 * mod_ripper                   12.4517 ms
 * mod_virus_authentium         57.2215 ms
 * mod_virus_networkassoci     290.3953 ms
 * mod_virus_sophos            296.4575 ms
 * mod_spam_bayes                3.0802 ms
 * mod_spam_cmae                43.6858 ms
 * mod_spam_heur                43.3588 ms
Gigabytes delivered per day: 1528.72
Gigabytes quarantined per day: 35.41
Percentage of messages quarantined: .0137
Percentage of messages denied: .8495
Percentage of messages delivered: .1367
Average inbound MTA load (9:00 snapshots): 13.62%
Average outbound MTA load (9:00 snapshots): 14.20%
Average messages quarantined per day: 1916889.19
Total quar DB size: 73 GB
Customers subscribing to MC: 9708




#################################################################################################
#################################################################################################
#################################################################################################
#################################################################################################

##
## Global variables
##
my $pair_dist = ();

##
## Create variables for CID processing
##
#
# 
#
my %ndx24 = ();
my %ingest24 = ();
my %segment = ();

#
# Non hashes
#
my $policydb = ();
my @logdb = ();
my @quardb = ();

##
## Get the global information needed
##
#
# Get IP of policy database server
#
	$policydb = `/mxl/bin/get_config_entry -f $PODCONFIG -S "pod/db?path"`;

	chomp($policydb);

#
# Get IPs of the other database servers (log and quarantine)
#
	open(LDB,"/root/dist/db.log") || die "/root/dist/db.log: $!";
	@logdb = <LDB>;
	close(LDB);

	chomp($logdb);

	open(QDB,"/root/dist/db.quar") || die "/root/dist/db.quar: $!";
	@quardb = <QDB>;
	close(QDB);

	chomp($quardb);

	if(($flags{'debug'}) || ($flags{'verbose'}))
	{
		print "\nfile inputs\n";
		print Dumper $policydb;
		print Dumper $logdb;
		print Dumper $quardb;
	}

#
# Get a list of mtas for this location
#
######
######
######
######
	open(MAS,"/root/dist/archiving_mas") || die "Cannot open /root/dist/archiving_mas: $!";
	@masips1 = <MAS>;
	close(MAS);

	@masips2 = `/usr/bin/ssh $pair_dist /bin/cat /root/dist/archiving_mas`;

	@masips = (@masips1, @masips2);
	chomp(@masips);

	foreach $srv_ip (@masips)
	{
		$srv_type{$srv_ip} = "mas";

		@ips = split(/\./,$srv_ip);

		$datacenter{$srv_ip} = "";

		switch ( $ips[1] )
		{
			case 11 { $datacenter{$srv_ip} = "Englewood"; }
			case 12 { $datacenter{$srv_ip} = "Denver"; }
			case 13 { $datacenter{$srv_ip} = "Amsterdam"; }
			case 14 { $datacenter{$srv_ip} = "Tokyo"; }
			case 15 { $datacenter{$srv_ip} = "Sydney"; }
			case 16 { $datacenter{$srv_ip} = "Auckland"; }
			case 17 { $datacenter{$srv_ip} = "London"; }
			case 18 { $datacenter{$srv_ip} = "Hong Kong"; }
			case 19 { $datacenter{$srv_ip} = "Singapore"; }
		}

		if ( $datacenter{$srv_ip} eq "" )
		{
			if ( $ips[2] == 106 )
			{
				$datacenter{$srv_ip} = "Englewood";
			}
			else
			{
				$datacenter{$srv_ip} = "Denver";
			}
		}

	}

#
# Get a list of solr servers from both sites
#
	open(SOLR,"/root/dist/archiving_solr") || die "Cannot open /root/dist/archiving_solr: $!";
	@solrips1 = <SOLR>;
	close(SOLR);

	@solrips2 = `/usr/bin/ssh $pair_dist /bin/cat /root/dist/archiving_solr`;

	@solrips = (@solrips1, @solrips2);
	chomp(@solrips);

	foreach $srv_ip (@solrips)
	{
		chomp($srv_ip);

		$srv_type{$srv_ip} = "solr";

		@ips = split(/\./,$srv_ip);

		$datacenter{$srv_ip} = "";

		switch ( $ips[1] )
		{
			case 11 { $datacenter{$srv_ip} = "Englewood"; }
			case 12 { $datacenter{$srv_ip} = "Denver"; }
			case 13 { $datacenter{$srv_ip} = "Amsterdam"; }
			case 14 { $datacenter{$srv_ip} = "Tokyo"; }
			case 15 { $datacenter{$srv_ip} = "Sydney"; }
			case 16 { $datacenter{$srv_ip} = "Auckland"; }
			case 17 { $datacenter{$srv_ip} = "London"; }
			case 18 { $datacenter{$srv_ip} = "Hong Kong"; }
			case 19 { $datacenter{$srv_ip} = "Singapore"; }
		}

		if ( $datacenter{$srv_ip} eq "" )
		{
			if ( $ips[2] == 106 )
			{
				$datacenter{$srv_ip} = "Englewood";
			}
			else
			{
				$datacenter{$srv_ip} = "Denver";
			}
		}
	}

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
	$archivedb = `/mxl/bin/get_config_entry -f $PODCONFIG -S "pod/arc_grid_db?path"`;
	chomp($archivedb);

	if(($flags{'debug'}) || ($flags{'verbose'}))
	{
		print "\narchive DB IP address\n";
		print Dumper $archivedb;
	}

##
#
# Get the list of CIDs from the DB server
#
	if( $flags{'test'} )
	{
		if ( $pair_dist =~ /10.1.107.1/ )
		{
			# pod 1
			@cids3 = qw ( 2087657000 2091798093 2004755105 2012820981 2071307165 2084509171 2076577354 );
		}
		else
		{
			# pod 2
			@cids3 = qw ( 44678636 33506819 33358612 28731319 );
		}
	}
	else
	{
		@cids1 = Arch::cids_from_mail_source();
		chomp(@cids1);

		@cids2 = `/mxl/sbin/dnsdirector solr list cid`;
		chomp(@cids2);

		@cids3 = (@cids1, @cids2);
		chomp(@cids3);

	}

	undef %seen;
	@cids3 = grep(!$seen{$_}++, @cids3);

	if(($flags{'debug'}) || ($flags{'verbose'}))
	{
		print "\ncids3 for validation\n";
		print Dumper \@cids3;
	}

#
# Validate all the CIDs and eliminate any that have not ingested any mail
#
	@cids = ();

	foreach $cid (@cids3) 
	{
		$cidseqnum = `/mxl/sbin/arseqcnt $cid`;

		if ( $cidseqnum > 0 )
		{
			push ( @cids, $cid );
		}
	}

	chomp(@cids);

	$cid_counter = 0;

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
	print "###################################################################################### \n";

##
## Get data and populate the CID table
##
	foreach $cid (@cids) 
	{
		print "Working CID: getting data\n";
		print Dumper $cid;

		$cid_counter++;

		if ( $flags{'debug'} )
		{ 
			if ( $cid_counter > $debug_cid ) 
			{ 
				next; 
			} 
		}

		$timestamp{$cid} = time();

		if (( $flags{'debug'} ) || ( $flags{'verbose'} ))
		{
			print "\ntimestamp\n";
			print Dumper \%timestamp;
		}

#
# Get IPs of ingest and search mas pools
#
		@ipsolr = (split(/\s+/,`/mxl/sbin/dnsdir-cust $cid solr | sed s/\\\\//" "/g | sed s/","/" "/g | sed s/"*"//g`));
		chomp(@ipsolr);

		@ingestmaspool = (split(/\s+/,`/mxl/sbin/dnsdir-cust $cid ingest.mas | sed s/","/" "/g`));
		chomp(@ingestmaspool);

		@searchmaspool = (split(/\s+/,`/mxl/sbin/dnsdir-cust $cid search.mas | sed s/","/" "/g`));
		chomp(@searchmaspool);

		if (( $flags{'debug'} ) || ( $flags{'verbose'} ))
		{
			print "\nsolr IPs\n";
			print Dumper \@ipsolr;
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
			$solrndxbklg{$cid} += `ssh $ip "su - mxl-archive -c '/usr/bin/find /var/tmp/index_queue/$cid/ -type f -print'" 2>/dev/null | wc -l`;
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
# Get the solr label for this CID
#
		$solrlabel{$cid} = `/mxl/sbin/dnsdirector solr list cid --cid $cid `;
		chomp($solrlabel{$cid});

		if (( $flags{'debug'} ) || ( $flags{'verbose'} ))
		{
			print "\nsolr label from DNS director\n";
			print Dumper \%solrlabel;
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

#
# Get the solr count for this CID
#
		if ( $solrseqnum{$cid} eq "" )
		{
			$solrcnt1{$cid} = 0;
			$solrcnt2{$cid} = 0;
		}
		else
		{
			$solrcnt1{$cid} = 0;
			$solrcnt2{$cid} = 0;

			foreach $ip (@ipsolr)
			{
				$scnt1 = "";
				$scnt2 = "";

				if ( grep(/$ip/,@solrips1) ge 1 )
				{
					$scnt1 = `/mxl/sbin/solr --solr $ip --cust=$cid count 2>/dev/null`;
					chomp($scnt1);
					if (( $scnt1 =~ /\D/ ) || ( $scnt1 eq "" ))
					{
						sleep(2);
						$scnt1 = `/mxl/sbin/solr --solr $ip --cust=$cid count 2>/dev/null`;
						chomp($scnt1);
						if (( $scnt1 =~ /\D/ ) || ( $scnt1 eq "" ))
						{
							$scnt1 = -1;
						}
					}
					$solrcnt1{$cid} += $scnt1;
				}
				else
				{
					$scnt2 = `/mxl/sbin/solr --solr $ip --cust=$cid count 2>/dev/null`;
					chomp($scnt1);
					if (( $scnt2 =~ /\D/ ) || ( $scnt2 eq "" ))
					{
						sleep(2);
						$scnt2 = `/mxl/sbin/solr --solr $ip --cust=$cid count 2>/dev/null`;
						chomp($scnt2);
						if (( $scnt2 =~ /\D/ ) || ( $scnt2 eq "" ))
						{
							$scnt2 = -1;
						}
					}
					$solrcnt2{$cid} += $scnt2;
				}
			}
		}

		chomp($solrcnt1{$cid});
		chomp($solrcnt2{$cid});

		if (( $flags{'debug'} ) || ( $flags{'verbose'} ))
		{
			print "\nsolr1 count\n";
			print Dumper \%solrcnt1;
			print "\nsolr2 count\n";
			print Dumper \%solrcnt2;
		}

#
# Get the solr segment count and solr reject count over all the solr(s) defined for this CID
#
		$solrseg1{$cid} = 0;
		$solrseg2{$cid} = 0;
		$solrej1{$cid} = 0;
		$solrej2{$cid} = 0;
		$sizemb1{$cid} = 0;
		$sizemb2{$cid} = 0;

		foreach $ip (@ipsolr)
		{
			if ( grep(/$ip/,@solrips1) ge 1 )
			{
				$solrseg1{$cid} += `ssh $ip "su - mxl-archive -c '/bin/ls -1 /mxl/msg_archive/solr/data/$cid/index'" 2>/dev/null | /bin/grep -c "_"`;
				chomp($solrseg1{$cid});

				$solrej1{$cid} += `ssh $ip "if [ -d /var/tmp/index_reject/solr-primary/$cid -o -d /var/tmp/index_reject/solr-secondary/$cid ] ; then /usr/bin/find /var/tmp/index_reject/solr-*/$cid  -type f -name 111111* -print | wc -l ; else echo 0;fi"`;
				chomp($solrej1{$cid});

				$size[0] = 0;

				@size = split(/\s+/,`ssh $ip "su - mxl-archive -c 'if [ -d /mxl/msg_archive/solr/data/$cid ] ; then /usr/bin/du -sm /mxl/msg_archive/solr/data/$cid ; else echo 0 ; fi'"`);

				$sizemb1{$cid} += $size[0];
				chomp($sizemb1{$cid});
			}
			else
			{
				$solrseg2{$cid} += `ssh $ip "su - mxl-archive -c '/bin/ls -1 /mxl/msg_archive/solr/data/$cid/index'" 2>/dev/null | /bin/grep -c "_"`;
				chomp($solrseg2{$cid});

				$solrej2{$cid} += `ssh $ip "if [ -d /var/tmp/index_reject/solr-primary/$cid -o -d /var/tmp/index_reject/solr-secondary/$cid ] ; then /usr/bin/find /var/tmp/index_reject/solr-*/$cid  -type f -name 111111* -print | wc -l ; else echo 0;fi"`;
				chomp($solrej2{$cid});

				$size[0] = 0;

				@size = split(/\s+/,`ssh $ip "su - mxl-archive -c 'if [ -d /mxl/msg_archive/solr/data/$cid ] ; then /usr/bin/du -sm /mxl/msg_archive/solr/data/$cid ; else echo 0 ; fi'"`);

				$sizemb2{$cid} += $size[0];
				chomp($sizemb2{$cid});
			}
		}

		if (( $flags{'debug'} ) || ( $flags{'verbose'} ))
		{
			print "\nsolr1 segment count\n";
			print Dumper \%solrseg1;
			print "\nsolr2 segment count\n";
			print Dumper \%solrseg2;
			print "\nsolr1 rejects\n";
			print Dumper \%solrej1;
			print "\nsolr2 rejects\n";
			print Dumper \%solrej2;
			print "\nsize on all solr1s\n";
			print Dumper \%sizemb1;
			print "\nsize on all solr2s\n";
			print Dumper \%sizemb2;
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

		$mscnt{$cid} = `/usr/bin/psql -At -h $policydb -U postgres mxl -c "select count(server_id) from arc_mail_source where id=$cid"`;
		chomp($mscnt{$cid});

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
		$ndx24{$cid} += $ndx[1];
		chomp($ndx24{$cid});
		
		if (( $flags{'debug'} ) || ( $flags{'verbose'} ))
		{
			print "\nnumber of indexed messages in last 24 hours\n";
			print Dumper \%ndx24;
		}

#
# Get number of messages ingested in last 24 hours
#
      @mailers = `/usr/bin/psql -At -h $policydb -U postgres mxl -c "select server_id from arc_mail_source where id = $cid"`;
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
		$severe1{$cid} = 0;
		$severe2{$cid} = 0;

		foreach $ip (@ipsolr)
		{
			if ( grep(/$ip/,@solrips1) ge 1 )
			{
				$severe1{$cid} += `ssh $ip /bin/grep -c SEVERE /var/log/mxl/tomcat/catalina.out`;
				$severe1{$cid} += `ssh $ip /bin/grep -c SEVERE /var/log/mxl/tomcat/catalina.out.1.gz`;
			}
			else
			{
				$severe2{$cid} += `ssh $ip /bin/grep -c SEVERE /var/log/mxl/tomcat/catalina.out`;
				$severe2{$cid} += `ssh $ip /bin/grep -c SEVERE /var/log/mxl/tomcat/catalina.out.1.gz`;
			}
		}
	
##
## Loop through all the mail sources for this CID
##
      foreach my $sid (@mailers) 
		{
			print "Working SID \n";
			print Dumper $sid;


         $ms_ref = Arch::ms_from_sid($sid);
         %ms_hash = %$ms_ref;

			$ms_timestamp{$sid} = time();
			chomp($ms_timestamp{$sid});

			$ms_status{$sid} = `/usr/bin/psql -At -h $policydb -U postgres mxl -c "select last_poll_status from arc_mail_source where server_id=$sid"`;
			chomp($ms_status{$sid});

			if ( $ms_status{$sid} eq "" )
			{
				$ms_status{$sid}=99;
				chomp($ms_status{$sid});
			}

			$ms_host{$sid} = $ms_hash{'server_host'};
			chomp($ms_host{$sid});

			$ms_user{$sid} = $ms_hash{'username'};
			chomp($ms_user{$sid});

			$ms_port{$sid} = $ms_hash{'server_port'};
			chomp($ms_port{$sid});

			$ms_type{$sid} = `/usr/bin/psql -At -h $policydb -U postgres mxl -c "select type_id from arc_mail_source where server_id=$sid"`;
			chomp($ms_type{$sid});

			$ms_name{$sid} = `/usr/bin/psql -At -h $policydb -U postgres mxl -c "select name from mxl_mail_source_type where type_id=$ms_type{$sid}"`;
			chomp($ms_name{$sid});

			$ms_desc{$sid} = `/usr/bin/psql -At -h $policydb -U postgres mxl -c "select description from mxl_mail_source_type where type_id=$ms_type{$sid}"`;
			chomp($ms_desc{$sid});

			$ms_ingest24{$sid} = `/usr/bin/psql -At -h $archivedb -U postgres mxl_archive -c "select sum(ingested_msg_count) from arc_mail_source_poll_log where ms_server_id=$sid and poll_stop > now() - '24 hours'::interval"`;
			chomp($ms_ingest24{$sid});

			if ( $ms_ingest24{$sid} eq "" )
			{
				$ms_ingest24{$sid}=0;
				chomp($ms_ingest24{$sid});
			}

			if ( ( $ms_status{$sid} == 0 ) || ( $ms_status{$sid} == 1 ) || ( $ms_status{$sid} == 13 ) )
			{
         	$cmd =   "/usr/local/bin/opsadmin/arc_check_ms.pl -c ".
               	"--host $ms_hash{'server_host'} ".
               	"--user $ms_hash{'username'} ".
               	"--port $ms_hash{'server_port'}";

         	$results = `$cmd`;
				chomp($results);

				if ( $results =~ m/^\d+$/ )
				{
					$ms_backlog{$sid} = int($results);
					chomp($ms_backlog{$sid});
				}
				else
				{
					$ms_backlog{$sid} = 5555555555;
				}
			}
			else
			{
					$ms_backlog{$sid} = 1111111111;
			}

			if (( $flags{'debug'} ) || ( $flags{'verbose'} ))
			{
				print "\nmail source timestamp\n";
            print Dumper \%ms_timestamp;
            print "\nmail source status\n";
            print Dumper \%ms_status;
            print "\nmail source host\n";
            print Dumper \%ms_host;
            print "\nmail source user\n";
            print Dumper \%ms_user;
            print "\nmail source port\n";
            print Dumper \%ms_port;
            print "\nmail source type\n";
            print Dumper \%ms_type;
            print "\nmail source name\n";
            print Dumper \%ms_name;
            print "\nmail source description\n";
            print Dumper \%ms_desc;
            print "\nmail source backlog\n";
            print Dumper \%ms_backlog;
         }

#
# ms_status meanings (return code from arfetchmail)
#
#
#        0     One  or more messages were successfully retrieved (or, if the -c option was selected,
#              were found waiting but not retrieved).
#    
#        1     There was no mail awaiting retrieval.  (There may have been old  mail  still  on  the
#              server but not selected for retrieval.)
#    
#        2     An  error  was encountered when attempting to open a socket to retrieve mail.  If you
#              don’t know what a socket is, don’t worry about it -- just treat this as an  ’unrecov-
#              erable  error’.   This error can also be because a protocol fetchmail wants to use is
#              not listed in /etc/services.
#    
#        3     The user authentication step failed.  This usually means that a  bad  user-id,  pass-
#              word, or APOP id was specified.  Or it may mean that you tried to run fetchmail under
#              circumstances where it did not have standard input attached to a terminal  and  could
#              not prompt for a missing password.
#    
#        4     Some sort of fatal protocol error was detected.
#    
#        5     There was a syntax error in the arguments to fetchmail.
#    
#        6     The run control file had bad permissions.
#    
#        7     There  was  an  error  condition  reported by the server.  Can also fire if fetchmail
#              timed out while waiting for the server.
#    
#        8     Client-side exclusion error.  This means  fetchmail  either  found  another  copy  of
#              itself  already  running,  or failed in such a way that it isn’t sure whether another
#              copy is running.
#    
#        9     The user authentication step failed because the server responded  "lock  busy".   Try
#              again  after a brief pause!  This error is not implemented for all protocols, nor for
#              all servers.  If not implemented for your server, "3" will be returned  instead,  see
#              above.   May  be  returned  when talking to qpopper or other servers that can respond
#              with "lock busy" or some similar text containing the word "lock".
#    
#			10    The fetchmail run failed while trying to do an SMTP port open or transaction.
#			
#			11    Fatal DNS error.  Fetchmail encountered an error while performing  a  DNS  lookup  at
#					startup and could not proceed.
#			
#			12    BSMTP batch file could not be opened.
#			
#			13    Poll terminated by a fetch limit (see the --fetchlimit option).
#			
#			14    Server busy indication.
#			
#			23    Internal error.  You should see a message on standard error with details.
#			


#
# Populate the mailsource table for this CID
#

      	$dbh = DBI->connect("dbi:Pg:dbname=watcher;host=localhost","postgres","") || die $DBI::errstr;

			$insert =  "insert into arc_mailsource (ms_epochtime,cid,sid,cstnam,ms_status,ms_host,ms_user,ms_port,ms_type,ms_name,ms_desc,ms_backlog,ms_ingest24) values (to_timestamp(?),?,?,?,?,?,?,?,?,?,?,?,?)";

			$sth = $dbh->prepare($insert);

			if ( $flags{'debug'} ) 
			{			# do not populate the database when debugging

				print "\ninsert command\n";
				print Dumper $insert;
				print "execute($ms_timestamp{$sid},$cid,$sid,$cstnam{$cid},$ms_status{$sid},$ms_host{$sid},$ms_user{$sid},$ms_port{$sid},$ms_type{$sid},$ms_name{$sid},$ms_desc{$sid},$ms_backlog{$sid},$ms_ingest24{$sid})";
			}
			else
			{
				if ( $flags{'verbose'} )
				{
					print "\ninsert command\n";
					print Dumper $insert;
					print Dumper $dbh;
					print Dumper $sth;
					print "execute($ms_timestamp{$sid},$cid,$sid,$cstnam{$cid},$ms_status{$sid},$ms_host{$sid},$ms_user{$sid},$ms_port{$sid},$ms_type{$sid},$ms_name{$sid},$ms_desc{$sid},$ms_backlog{$sid},$ms_ingest24{$sid})";
				}

	
				$sth->execute($ms_timestamp{$sid},$cid,$sid,$cstnam{$cid},$ms_status{$sid},$ms_host{$sid},$ms_user{$sid},$ms_port{$sid},$ms_type{$sid},$ms_name{$sid},$ms_desc{$sid},$ms_backlog{$sid},$ms_ingest24{$sid}) || die $DBI::errstr;

				$sth->finish();
				$dbh->disconnect();
	
			}
		} 	# end mail source loop

##
## End mail source loop
##
		
	}  # end of cid loop

##
## End CID loop
##

##
## Get all server specific data
##
	foreach my $ip (keys %datacenter) 
	{
		chomp($ip);

		$srv_timestamp{$ip} = time();

		if (( $flags{'debug'} ) || ( $flags{'verbose'} ))
		{
			print "\nserver timestamp\n";
			print Dumper \%srv_timestamp;
			print "\nserver type\n";
			print Dumper \%srv_type;
		}

		if ( $srv_type{$ip} eq "solr" )
		{

#
# for a solr get the count from DNSdirector
#
			$DNSret = `/mxl/sbin/dnsdirector solr verify -v -v -v | /bin/grep $ip `;
			chomp($DNSret);

			@DNSlabel = split(/\s+/,$DNSret);

			$DNScnt{$ip} = `/mxl/sbin/dnsdirector solr verify -v -v -v | /bin/grep -vw $ip | /bin/grep -c $DNSlabel[1]`;
			chomp($DNScnt{$ip});

			if (( $flags{'debug'} ) || ( $flags{'verbose'} ))
			{
				print "\nDNS director count\n";
				print Dumper \%DNScnt;
			}

#
# for a solr get the directory count from the server
#
			$dircnt{$ip} = `ssh $ip "su - mxl-archive -c 'ls -ld /mxl/msg_archive/solr/data/[0-9]*[0-9] | wc -l'"`;
			chomp($dircnt{$ip});

			if (( $flags{'debug'} ) || ( $flags{'verbose'} ))
			{
				print "\nDirectory (customer) count\n";
				print Dumper \%dircnt;
			}

#
# for a solr Get the local disk usage for the server
#
			$diskdfret = `ssh $ip /bin/df -m /var/msg_archive/solr 2>/dev/null | grep /var`;
			@diskdf = split(/\s+/,$diskdfret);
			$diskfree{$ip} = $diskdf[3];
			chomp($diskfree{$ip});

			if (( $flags{'debug'} ) || ( $flags{'verbose'} ))
			{
				print "\nlocal disk free space\n";
				print Dumper \%diskfree;
			}
		}
		else
		{
			$DNScnt{$ip} = 0;
			$dircnt{$ip} = 0;
			$diskfree{$ip} = 0;
		}

	} 	# end server loop

##
## End loop for server data
##


##
## Handle the Archiving shared storage
##
#
# Get the list of mount points under /mxl/msg_archive/mnt for all ingest and search mas'
#
	foreach $ip (@masips)
	{
		chomp($ip);

		@srvmp = (`ssh $ip grep /mxl/msg_archive/mas /etc/fstab | grep -v ^#`);
		chomp(@srvmp);

		foreach $line (@srvmp)
		{
			@fields = (split(/\s+/,$line));

			@nfsmp = (@nfsmp, $fields[1]);
			chomp(@nfsmp);
	
			@allmpips = (@allmpips, $ip);	
			chomp(@allmpips);
		}
	}

	undef %seen;
	@nfsmp = grep(!$seen{$_}++, @nfsmp);

	undef %seen;
	@allmpips = grep(!$seen{$_}++, @allmpips);

	if (( $flags{'debug'} ) || ( $flags{'verbose'} ))
	{
		print "\nmount points to check\n";
		print Dumper \@nfsmp;
		print "\nmount point ip addresses\n";
		print Dumper \@allmpips;
	}

##
## End loop for gathering NFS mount data
##

##
## Get the storage data
##
	foreach $mp (@nfsmp)
	{
		foreach $ip (@allmpips)
		{
			chomp($ip);

			$nfs_timestamp{$ip.':'.$mp} = time();

			$nfsret = `ssh $ip /bin/df -m $mp 2>/dev/null | grep $mp`;

			if (( $flags{'debug'} ) || ( $flags{'verbose'} ))
			{
				print "\ndf return\n";
				print Dumper \$nfsret;
			}

			if ( $nfsret eq "" )
			{
				$nfsused{$ip.':'.$mp} = 0;
				$nfsfree{$ip.':'.$mp} = 0;
			}
			else
			{
				@nfsstat = split(/\s+/,$nfsret);

				$nfsused{$ip.':'.$mp} = $nfsstat[2];
				$nfsfree{$ip.':'.$mp} = $nfsstat[3];
	
				if (( $flags{'debug'} ) || ( $flags{'verbose'} ))
				{
					print "\nNFS used\n";
					print Dumper \%nfsused;
					print "\nNFS free\n";
					print Dumper \%nfsfree;
					print "\nNFS stat array\n";
					print Dumper \@nfsstat;
				}
			}
		}
	} 	#end NFS loop

##
## End loop for gathering NFS mount point data
##

##
## In debug but NOT verbose dump all hashes
##
if (( $flags{'debug'} ) && ( ! $flags{'verbose'} ))
{

   print "\ncids\n";
   print Dumper \@cids;
   print "\ntimestamp\n";
   print Dumper \%timestamp;
   print "\ningest mas IPs\n";
   print Dumper \@ingestmaspool;
   print "\nsearch mas IPs\n";
   print Dumper \@searchmaspool;
   print "\ncustomer name\n";
   print Dumper \%cstnam;
   print "\nsolr1 count\n";
   print Dumper \%solrcnt1;
   print "\nsolr2 count\n";
   print Dumper \%solrcnt2;
   print "\nsolr1 segment count\n";
   print Dumper \%solrseg1;
   print "\nsolr2 segment count\n";
   print Dumper \%solrseg2;
   print "\nsolr1 rejects\n";
   print Dumper \%solrej1;
   print "\nsolr2 rejects\n";
   print Dumper \%solrej2;
   print "\nmas rejects\n";
   print Dumper \%masrej;
   print "\nuser count\n";
   print Dumper \%usrcnt;
   print "\nmail source count\n";
   print Dumper \%mscnt;
   print "\nmailers\n";
   print Dumper \@mailers;
   print "\nresults\n";
   print Dumper $results;
	print "\nsize on all solr1s\n";
	print Dumper \%sizemb1;
	print "\nsize on all solr2s\n";
	print Dumper \%sizemb2;
   print "\nmail source count\n";
   print Dumper $count;
   print "\nmail source status\n";
   print Dumper \%ms_status;
   print "\nsolr sequence number\n";
   print Dumper \%solrseqnum;
   print "\nsolr index backlog\n";
   print Dumper \%solrndxbklg;
   print "\nserver ips\n";
   print Dumper $srv_ip;
   print "\ndata center\n";
   print Dumper \%datacenter;
   print "\nserver type\n";
   print Dumper \%srv_type;
   print "\nserver timestamp\n";
   print Dumper \%srv_timestamp;
   print "\nDNS director count\n";
   print Dumper \%DNScnt;
   print "\nDirectory (customer) count\n";
   print Dumper \%dircnt;
   print "\nlocal disk free space\n";
   print Dumper \%diskfree;
   print "\ndf return\n";
   print Dumper \@nfsstat;
   print "\nNFS used\n";
   print Dumper \%nfsused;
   print "\nNFS free\n";
   print Dumper \%nfsfree;
   print "\nmail source timestamp\n";
   print Dumper \%ms_timestamp;
   print "\nmail source status\n";
   print Dumper \%ms_status;
   print "\nmail source host\n";
   print Dumper \%ms_host;
   print "\nmail source user\n";
   print Dumper \%ms_user;
   print "\nmail source port\n";
   print Dumper \%ms_port;
   print "\nmail source type\n";
   print Dumper \%ms_type;
   print "\nmail source name\n";
   print Dumper \%ms_name;
   print "\nmail source description\n";
   print Dumper \%ms_desc;
   print "\nmail source backlog\n";
   print Dumper \%ms_backlog;
   print "\nsolr1 severe errors\n";
   print Dumper \%severe1;
   print "\nsolr2 severe errors\n";
   print Dumper \%severe2;

}

##
## Populate the Archiving tables in the watcher database
##
$cid_counter = 0;

##
## Populate the CID table
##
   foreach my $cid (@cids)
   {
		print "Inserting CID \n";
		print Dumper $cid;

      $cid_counter++;

      if ( $flags{'debug'} )
      {
         if ( $cid_counter > $debug_cid )
         {
            next;
         }
      }

      $dbh = DBI->connect("dbi:Pg:dbname=watcher;host=localhost","postgres","") || die $DBI::errstr;

      $insert = "insert into arc_cid (epochtime,cid,solrcnt1,solrcnt2,cstnam,solrlabel,solrseg1,solrseg2,solrej1,solrej2,sizemb1,sizemb2,masrej,mscnt,usrcnt,solrndxbklg,solrseqnum,ingest24,ndx24,severe1,severe2) values (to_timestamp(?),?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

      $sth = $dbh->prepare($insert);

      if ( $flags{'debug'} )
      {        # do not populate the database when debugging

         print "\ninsert command\n";
         print Dumper $insert;
         print "execute($timestamp{$cid},$cid,$solrcnt1{$cid},$solrcnt2{$cid},$cstnam{$cid},$solrlabel{$cid},$solrseg1{$cid},$solrseg2{$cid},$solrej1{$cid},$solrej2{$cid},$sizemb1{$cid},$sizemb2{$cid},$masrej{$cid},$mscnt{$cid},$usrcnt{$cid},$solrndxbklg{$cid},$solrseqnum{$cid},$ingest24{$cid},$ndx24{$cid},$severe1{$cid},$severe2{$cid})";
      }

      else
      {
         if ( $flags{'verbose'} )
         {
            print "\ninsert command\n";
            print Dumper $insert;
            print Dumper $dbh;
            print Dumper $sth;
            print "execute($timestamp{$cid},$cid,$solrcnt1{$cid},$solrcnt2{$cid},$cstnam{$cid},$solrlabel{$cid},$solrseg1{$cid},$solrseg2{$cid},$solrej1{$cid},$solrej2{$cid},$sizemb1{$cid},$sizemb2{$cid},$masrej{$cid},$mscnt{$cid},$usrcnt{$cid},$solrndxbklg{$cid},$solrseqnum{$cid},$ingest24{$cid},$ndx24{$cid},$severe1{$cid},$severe2{$cid})";
         }

         $sth->execute($timestamp{$cid},$cid,$solrcnt1{$cid},$solrcnt2{$cid},$cstnam{$cid},$solrlabel{$cid},$solrseg1{$cid},$solrseg2{$cid},$solrej1{$cid},$solrej2{$cid},$sizemb1{$cid},$sizemb2{$cid},$masrej{$cid},$mscnt{$cid},$usrcnt{$cid},$solrndxbklg{$cid},$solrseqnum{$cid},$ingest24{$cid},$ndx24{$cid},$severe1{$cid},$severe2{$cid}) || die $DBI::errstr;

         $sth->finish();
         $dbh->disconnect();

      }

	}  #end write CID information loop

##
## End loop to populate CID table
##

##
## Populate the Server table
##
	foreach $ip (keys %datacenter) 
	{
		$insert = "psql -U postgres -d watcher -h localhost -c \"insert into arc_server (epochtime,srv_ip,srv_type,datacenter,DNScnt,dircnt,diskfree) values (to_timestamp($srv_timestamp{$ip}),\'$ip\',\'$srv_type{$ip}\',\'$datacenter{$ip}\',$DNScnt{$ip},$dircnt{$ip},$diskfree{$ip})\"";

		if ( $flags{'debug'} ) 
		{			# do not populate the database when debugging
			print "\ninsert command\n";
			print Dumper $insert;
		}
		else
		{
			if ( $flags{'verbose'} )
			{
				print "\ninsert command\n";
				print Dumper $insert;
			}
	     	system("$insert");
		}
	} 	#end write Server information loop

##
## End loop to populate server table
##

##
## Populate the Storage table
##
	foreach $mp (@nfsmp)
	{
		foreach $ip (@allmpips)
		{
			chomp($ip);

			$insert = "psql -U postgres -d watcher -h localhost -c \"insert into arc_storage (epochtime,ip,mp,nfsused,nfsfree) values (to_timestamp($nfs_timestamp{$ip.':'.$mp}),\'$ip\',\'$mp\',$nfsused{$ip.':'.$mp},$nfsfree{$ip.':'.$mp})\"";

			if ( $flags{'debug'} ) 
			{			# do not populate the database when debugging
				print "\ninsert command\n";
				print Dumper $insert;
			}
			else
			{
				if ( $flags{'verbose'} )
				{
					print "\ninsert command\n";
					print Dumper $insert;
				}
      		system("$insert");
			}
		}
	}	#end write Storage information loop

##
## End loop to populate storage information
##

##
## Put an entry to mark the last successful completion
##
	if ( ! $flags{'test'} )
	{
		my $lr_timestamp = time();

		$insert = "psql -U postgres -d watcher -h localhost -c \"insert into arc_lastrun (epochtime) values (to_timestamp($lr_timestamp))\"";

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
	GetOptions(
		'debug|d'       => \$flags{'debug'},
		'test|t'       => \$flags{'test'},
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
    -t, --test                Test Mode (limited CIDs)
    -h, --help                Print this help
EOF

        print $usage;
}







#################################################################################################
#################################################################################################
#################################################################################################
#################################################################################################





#!/bin/bash
# File: BAR_Metrics.sh
# Desc: Fetches various metrics for the BAR
# Code: by Shinto, 2008-03-04 last update

# Print usage information if requested
if [[ $1 == "-h" || $1 == "--help" || ! $1 ]]; then
	echo " Generate BAR metrics for a given month.  Can take a couple hours to run."
	echo
	echo "Usage: $0 [server names] [YYYY-MM]"
	echo
	echo "       [server names] is a parameter which describes the servers"
	echo "       you want to average.  Assume (but don't include) a * at"
	echo "       the end. For example: 'p01c11' will load up p01c11*"
	echo
	echo "       [YYYY-MM] is the year and month for which you want to"
	echo "       generate BAR metrics.  You can also specify 'lastmonth'"
	echo "       or leave it blank for the current month."
	exit
fi

# Bandwidth 95th Percentile
if [[ $pod -le 2 ]]; then
	bw95th=`wget -O '-' --quiet --user MXLogic --password MXLOPS1! "http://mrtg.data393.net/mrtg.cgi?rtr=aggregate%2Fmx_logic2.cfg&bars=Cami&page=graph&xgtype=m&xgstyle=l2&if=_summary_&xmtype=options" | grep 95th |sed 's/<[^>]*>/ /g;s/  / /g;s/.*: /Out: /;s/bps /bps, In: /'`
	echo "Bandwidth Utilization (Data393), 95th percentile: $bw95th"
fi

# Average Worker Counts (kinda slow)
if [[ $pod -le 2 ]]; then
	avg_worker_counts1=`/usr/local/bin/opsadmin/avg_worker_count.sh $box $month`
	box2=`echo $box |sed 's/11/12/'`
	avg_worker_counts2=`ssh 10.$pod.107.1 "/usr/local/bin/opsadmin/avg_worker_count.sh $box2 $month"`
	echo $avg_worker_counts1 |sed 's/:/ (Data393):/'
	echo $avg_worker_counts2 |sed 's/:/ (Level 3): /'
else
	avg_worker_counts=`/usr/local/bin/opsadmin/avg_worker_count.sh $box $month`
	echo $avg_worker_counts
fi

# Average Connection Counts (kinda slow)
if [[ $pod -le 2 ]]; then
	avg_connection_counts1=`/usr/local/bin/opsadmin/avg_connection_count.sh $box $month`
	box2=`echo $box |sed 's/11/12/'`
	avg_connection_counts2=`ssh 10.$pod.107.1 "/usr/local/bin/opsadmin/avg_connection_count.sh $box2 $month"`
	echo $avg_connection_counts1 |sed 's/:/ (Data393):/'
	echo $avg_connection_counts2 |sed 's/:/ (Level 3): /'
else
	if [[ $pod -le 2 || $pod -ge 200 ]]; then
		avg_connection_counts=`/usr/local/bin/opsadmin/avg_connection_count.sh $box $month`
		echo $avg_connection_counts
	fi
fi

# Time to MTA failure per Pod in minutes (kinda slow)
if [[ $pod -le 3 ]]; then
	box_mta="$box"m
else
	box_mta=$box
fi
if [[ $pod -le 3 ]]; then
	box_mta2=`echo $box_mta |sed 's/11/12/'`
	time_to_mta_failure1=`/usr/local/bin/opsadmin/avg_crash_count.sh mta $box_mta $month |grep minutes |sed 's/.*service: //'`
	time_to_mta_failure2=`ssh 10.$pod.107.1 "/usr/local/bin/opsadmin/avg_crash_count.sh mta $box_mta2 $month |grep minutes |sed 's/.*service: //'"`
	echo "Time to MTA Failure (Data393): $time_to_mta_failure1"
	echo "Time to MTA Failure (Level 3): $time_to_mta_failure2"
else
	time_to_mta_failure=`/usr/local/bin/opsadmin/avg_crash_count.sh mta $box_mta $month |grep minutes |sed 's/.*service: //'`
	echo "Time to MTA Failure: $time_to_mta_failure"
fi	

# Service Level Agreement (SLA) monitoring
if [[ $pod -le 2 ]]; then
	slas=`ssh 10.$pod.106.2 "/usr/local/TESLA/tesla_advanced.pl $month"`
else
	slas=`/usr/local/TESLA/tesla_advanced.pl $month`
fi
echo "SLAs for pod $pod:"
echo "$slas"

# Total messages processed for the month
echo "Total # of Msg Processed (Month): $messages_total"

# Average inbound messages per day
messages_per_day=`echo "scale=2;$messages_total/$days_in_month" |bc`
echo "Average inbound messages per day: $messages_per_day"

# Average outbound messages per day (slow)
for database in $logdb; do
	let outbound_messages+=`/usr/bin/psql -h $database -U postgres mxl_log -P tuples_only -P format=unaligned -c "SELECT SUM(CASE WHEN field='Messages Total' THEN value ELSE 0 END) FROM mxl_log_accum_out WHERE TO_CHAR(created,'YYYY-MM')='$month' AND domain!='system' AND domain!='bounce.mxlogic.com'"`
done
avg_outbound_per_day=`echo "scale=2;$outbound_messages/$days_in_month" |bc`
echo "Average outbound messages per day: $avg_outbound_per_day"

# Time to process message
if [[ $pod -le 3 ]]; then
	time_to_process=`/usr/local/bin/opsadmin/avg_message_timing.sh $box $month`
	echo $time_to_process
fi

# Module timing
if [[ $pod -le 3 ]]; then
	module_timing=`/usr/local/bin/opsadmin/avg_process_timing.pl $month`
	echo "$module_timing"
fi

# GB Delivered per day (slow)
for database in $logdb; do
	let bytes_delivered+=`/usr/bin/psql -h $database -U postgres mxl_log -P tuples_only -P format=unaligned -c "SELECT SUM(CASE WHEN field='Bytes Delivered' THEN value ELSE 0 END) FROM mxl_log_accum WHERE TO_CHAR(created,'YYYY-MM')='$month' AND domain!='system' AND domain!='bounce.mxlogic.com'"`
done
gb_delivered=`echo "scale=2;($bytes_delivered/(1024^3))/$days_in_month" |bc`
echo "Gigabytes delivered per day: $gb_delivered"

# GB Quarantined per day (slow)
for database in $logdb; do
	let bytes_quarantined+=`/usr/bin/psql -h $database -U postgres mxl_log -P tuples_only -P format=unaligned -c "SELECT SUM(CASE WHEN field='Bytes Quarantined' THEN value ELSE 0 END) FROM mxl_log_accum WHERE TO_CHAR(created,'YYYY-MM')='$month' AND domain!='system' AND domain!='bounce.mxlogic.com'"`
done
gb_quarantined=`echo "scale=2;($bytes_quarantined/(1024^3))/$days_in_month" |bc`
echo "Gigabytes quarantined per day: $gb_quarantined"

# Quarantined/Delivered mail percentage (slow)
for database in $logdb; do
	messages_quar_deliv=`/usr/bin/psql -h $database -U postgres mxl_log -P tuples_only -P format=unaligned -c "SELECT SUM(CASE WHEN field='Messages Quarantined' THEN value ELSE 0 END), SUM(CASE WHEN field='Messages Delivered' THEN value ELSE 0 END) FROM mxl_log_accum WHERE TO_CHAR(created,'YYYY-MM')='$month' AND domain!='system' AND domain!='bounce.mxlogic.com'"`
	let messages_quarantined+=`echo $messages_quar_deliv |cut -d'|' -f1`
	let messages_delivered+=`echo $messages_quar_deliv |cut -d'|' -f2`
done
let messages_denied=$messages_total-$messages_quarantined-$messages_delivered
perc_quarantined=`echo "scale=4;$messages_quarantined/$messages_total" |bc`
perc_delivered=`echo "scale=4;$messages_delivered/$messages_total" |bc`
perc_denied=`echo "scale=4;$messages_denied/$messages_total" |bc`
echo "Percentage of messages quarantined: $perc_quarantined"
echo "Percentage of messages denied: $perc_denied"
echo "Percentage of messages delivered: $perc_delivered"

# Average inbound MTA load (9:00 snapshot) per day
total_inbound_load=`grep $month /tmp/900snapshot.log |grep inbound |sed 's/.*bound //g;s/%//' |tr "\n" "+" |sed 's/+$/\n/' |bc`
average_inbound_load=`echo "scale=2;$total_inbound_load/$days_in_month" |bc`
echo "Average inbound MTA load (9:00 snapshots): $average_inbound_load%"

# Average outbound MTA load (9:00 snapshot)
if [[ $pod -lt 3 ]]; then
	total_outbound_load=`grep $month /tmp/900snapshot.log |grep outbound |sed 's/.*bound //g;s/%//' |tr "\n" "+" |sed 's/+$/\n/' |bc`
	average_outbound_load=`echo "scale=2;$total_outbound_load/$days_in_month" |bc`
	echo "Average outbound MTA load (9:00 snapshots): $average_outbound_load%"
fi

# Average messages quarantined per day
quarantined_per_day=`echo "scale=2;$messages_quarantined/$days_in_month" |bc`
echo "Average messages quarantined per day: $quarantined_per_day"

# Total DB size (quar DB?)
if [ -e /root/dist/db.quar ]; then
	quardb=`/root/dist/docommand.sh /root/dist/db.quar "df -h /mxl/var/pgsql/data |tail -1 |sed 's/ \+/ /g' |cut -d' ' -f3" |grep G |sed 's/G//' |tr "\n" "+" |sed 's/+$/\n/' |bc`
else
	quar_db_sizes=`ssh $quardb "df -h /mxl/var/pgsql/data |tail -1 |sed 's/ \+/ /g' |cut -d' ' -f3" |grep G |sed 's/G//'`
fi
echo "Total quar DB size: $quar_db_sizes GB"

# Number of SQR reports generated (total or average?  Try average)
#for reports in `zgrep "Reports : " /var/log/mxl/rotate/spam_report.log.200612* |sed "s/.* : //"`; do let count++; let total+=$reports; done; echo "scale=2;$total/$count" |bc

# (Average?) time of completion of SQR reports

if [[ $pod -lt 3 ]]; then
	mc_customers=`/usr/bin/psql -At -h $pol_db -U postgres -d mxl -c "SELECT COUNT (DISTINCT(name)) FROM mxl_customer WHERE customer_id IN (SELECT DISTINCT(customer_id) FROM mxl_domain WHERE domain_id IN (SELECT domain_id FROM mxl_pkg_sub WHERE in_pkg IN (SELECT pkg_id FROM mxl_pkg_def WHERE services LIKE '%Dr=Y%')))"`
	echo "Customers subscribing to MC: $mc_customers"
fi

if [[ $pod -lt 3 ]]; then
	echo
	echo "--[ WDS ]-----------------------------------------"
	# Total # of users
	wds_users=`/usr/bin/psql -h $pol_db -U postgres mxl -P tuples_only -P format=unaligned -c "SELECT sum(c.wds_billed_users_qty) FROM wds_pkg_sub s, wds_pkg_def p, mxl_customer c, mxl_reseller r WHERE s.pkg_id = p.pkg_id AND s.id = c.customer_id AND c.reseller_id = r.reseller_id AND c.name NOT LIKE 'Mxlogic QA%'"`
	echo "Total WDS Seat Count: $wds_users"
	#now handled by root's crontab
	#echo "$wds_users" > /root/wds_seat_count.pod${pod}
	#scp -p /root/wds_seat_count.pod${pod} admin:/home/www/html/dashiki >/dev/null 2>&1

# no more bluecoats so this isn't valid
#	# Total Mbps of traffic through WDS (95th Percentile)
#	secs_start=`date -d $month-01 +%s`
#	secs_end=`date -d $month-$days_in_month +%s`
#	rrd_out=`/usr/local/rrdtool/bin/rrdtool fetch /usr/local/rrdtool/nagios-trending/rrds/WDS_SG_Bandwidth/aggregate.WDS_SG_Bandwidth.rrd LAST -s $secs_start -e $secs_end |sed 's/.*:  //;s/e+/\*10\^/;s/ .*//' |grep 10`
#	rrd_count=`echo "$rrd_out" |wc -l`
#	nf=`echo "$rrd_count*0.95" |bc |sed 's/\..*//'`
#	wds_95=`for sample in $rrd_out; do echo "$sample" |bc; done | sort -n |head -$nf |tail -1`
#	wds_95=`echo "scale=2;($wds_95*8)/(1024^2)" |bc`
#	echo "95th percentile WDS bandwidth: $wds_95 Mbps"
#
#	# Peak Mbps traffic through WDS
#	wds_peak=`for sample in $rrd_out; do echo "$sample" |bc; done | sort -n |tail -1`
#	wds_peak=`echo "scale=2;($wds_peak*8)/(1024^2)" |bc`
#	echo "Peak WDS bandwidth: $wds_peak Mbps"

	# WDS Latency
	/usr/local/bin/opsadmin/wds_latency_average.pl $month

	########Other WDS Reports##########
	# last 4 hours
	printrrd  "WDS_Access_Around_World_Portal/p01c06i001.WDS_Access_Around_World_Portal.rrd" "4hours" "MIN"  "WDS Access from around the world to reach Portal" "(in ms)"
	printrrd  "WDS_Access_Around_World_Portal/p01c06i001.WDS_Access_Around_World_Portal.rrd" "4hours" "MAX"  "WDS Access from around the world to reach Portal" "(in ms)"
	printrrd  "WDS_Access_Around_World_Portal/p01c06i001.WDS_Access_Around_World_Portal.rrd" "4hours" "AVERAGE"  "WDS Access from around the world to reach Portal" "(in ms)"

	# last day
	printrrd  "WDS_Access_Around_World_Portal/p01c06i001.WDS_Access_Around_World_Portal.rrd" "1day" "MIN"  "WDS Access from around the world to reach Portal" "(in ms)"
	printrrd  "WDS_Access_Around_World_Portal/p01c06i001.WDS_Access_Around_World_Portal.rrd" "1day" "MAX"  "WDS Access from around the world to reach Portal" "(in ms)"
	printrrd  "WDS_Access_Around_World_Portal/p01c06i001.WDS_Access_Around_World_Portal.rrd" "1day" "AVERAGE"  "WDS Access from around the world to reach Portal" "(in ms)"

	# last week
	printrrd  "WDS_Access_Around_World_Portal/p01c06i001.WDS_Access_Around_World_Portal.rrd" "1week" "MIN"  "WDS Access from around the world to reach Portal" "(in ms)"
	printrrd  "WDS_Access_Around_World_Portal/p01c06i001.WDS_Access_Around_World_Portal.rrd" "1week" "MAX"  "WDS Access from around the world to reach Portal" "(in ms)"
	printrrd  "WDS_Access_Around_World_Portal/p01c06i001.WDS_Access_Around_World_Portal.rrd" "1week" "AVERAGE"  "WDS Access from around the world to reach Portal" "(in ms)"

	# last month
	printrrd  "WDS_Access_Around_World_Portal/p01c06i001.WDS_Access_Around_World_Portal.rrd" "1month" "MIN"  "WDS Access from around the world to reach Portal" "(in ms)"
	printrrd  "WDS_Access_Around_World_Portal/p01c06i001.WDS_Access_Around_World_Portal.rrd" "1month" "MAX"  "WDS Access from around the world to reach Portal" "(in ms)"
	printrrd  "WDS_Access_Around_World_Portal/p01c06i001.WDS_Access_Around_World_Portal.rrd" "1month" "AVERAGE"  "WDS Access from around the world to reach Portal" "(in ms)"

	# last year
	printrrd  "WDS_Access_Around_World_Portal/p01c06i001.WDS_Access_Around_World_Portal.rrd" "1year" "MIN"  "WDS Access from around the world to reach Portal" "(in ms)"
	printrrd  "WDS_Access_Around_World_Portal/p01c06i001.WDS_Access_Around_World_Portal.rrd" "1year" "MAX"  "WDS Access from around the world to reach Portal" "(in ms)"
	printrrd  "WDS_Access_Around_World_Portal/p01c06i001.WDS_Access_Around_World_Portal.rrd" "1year" "AVERAGE"  "WDS Access from around the world to reach Portal" "(in ms)"

	#########################Console
	# last 4 hours
	printrrd  "WDS_Access_Around_World_Console/p02c06i001.WDS_Access_Around_World_Console.rrd" "4hours" "MIN"  "WDS Access from around the world to reach Console" "(in ms)"
	printrrd  "WDS_Access_Around_World_Console/p02c06i001.WDS_Access_Around_World_Console.rrd" "4hours" "MAX"  "WDS Access from around the world to reach Console" "(in ms)"
	printrrd  "WDS_Access_Around_World_Console/p02c06i001.WDS_Access_Around_World_Console.rrd" "4hours" "AVERAGE"  "WDS Access from around the world to reach Console" "(in ms)"

	# last day
	printrrd  "WDS_Access_Around_World_Console/p02c06i001.WDS_Access_Around_World_Console.rrd" "1day" "MIN"  "WDS Access from around the world to reach Console" "(in ms)"
	printrrd  "WDS_Access_Around_World_Console/p02c06i001.WDS_Access_Around_World_Console.rrd" "1day" "MAX"  "WDS Access from around the world to reach Console" "(in ms)"
	printrrd  "WDS_Access_Around_World_Console/p02c06i001.WDS_Access_Around_World_Console.rrd" "1day" "AVERAGE"  "WDS Access from around the world to reach Console" "(in ms)"

	# last week
	printrrd  "WDS_Access_Around_World_Console/p02c06i001.WDS_Access_Around_World_Console.rrd" "1week" "MIN"  "WDS Access from around the world to reach Console" "(in ms)"
	printrrd  "WDS_Access_Around_World_Console/p02c06i001.WDS_Access_Around_World_Console.rrd" "1week" "MAX"  "WDS Access from around the world to reach Console" "(in ms)"
	printrrd  "WDS_Access_Around_World_Console/p02c06i001.WDS_Access_Around_World_Console.rrd" "1week" "AVERAGE"  "WDS Access from around the world to reach Console" "ms"

	# last month
	printrrd  "WDS_Access_Around_World_Console/p02c06i001.WDS_Access_Around_World_Console.rrd" "1month" "MIN"  "WDS Access from around the world to reach Console" "(in ms)"
	printrrd  "WDS_Access_Around_World_Console/p02c06i001.WDS_Access_Around_World_Console.rrd" "1month" "MAX"  "WDS Access from around the world to reach Console" "(in ms)"
	printrrd  "WDS_Access_Around_World_Console/p02c06i001.WDS_Access_Around_World_Console.rrd" "1month" "AVERAGE"  "WDS Access from around the world to reach Console" "(in ms)"

	# last year
	printrrd  "WDS_Access_Around_World_Console/p02c06i001.WDS_Access_Around_World_Console.rrd" "1year" "MIN"  "WDS Access from around the world to reach Console" "(in ms)"
	printrrd  "WDS_Access_Around_World_Console/p02c06i001.WDS_Access_Around_World_Console.rrd" "1year" "MAX"  "WDS Access from around the world to reach Console" "(in ms)"
	printrrd  "WDS_Access_Around_World_Console/p02c06i001.WDS_Access_Around_World_Console.rrd" "1year" "AVERAGE"  "WDS Access from around the world to reach Console" "(in ms)"

	#########################Viruses blocked, no aggregate
	printrrd  WDS_AV_NumVirusesCaught/p0*v032.WDS_AV_NumVirusesCaught.rrd "4hours" "LAST"  "Number of Viruses caught so far" ""
	printrrd  WDS_AV_NumVirusesCaught/p0*v032.WDS_AV_NumVirusesCaught.rrd "4hours" "AVERAGE"  "Number of Viruses cumulatively caught" ""
	printrrd  WDS_AV_NumVirusesCaught/p0*v032.WDS_AV_NumVirusesCaught.rrd "1day" "AVERAGE"  "Number of Viruses cumulatively caught" ""
	printrrd  WDS_AV_NumVirusesCaught/p0*v032.WDS_AV_NumVirusesCaught.rrd "1week" "AVERAGE"  "Number of Viruses cumulatively caught" ""
	printrrd  WDS_AV_NumVirusesCaught/p0*v032.WDS_AV_NumVirusesCaught.rrd "1month" "AVERAGE"  "Number of Viruses cumulatively caught" ""
	printrrd  WDS_AV_NumVirusesCaught/p0*v032.WDS_AV_NumVirusesCaught.rrd "1year" "AVERAGE"  "Number of Viruses cumulatively caught" ""

	#########################Files Scanned, no aggregate
	printrrd  WDS_AV_NumScannedFiles/p0*v032.WDS_AV_NumScannedFiles.rrd "4hours" "LAST"  "Number of Files Scanned so far" ""
	printrrd  WDS_AV_NumScannedFiles/p0*v032.WDS_AV_NumScannedFiles.rrd "4hours" "AVERAGE"  "Number of Files Scanned" ""
	printrrd  WDS_AV_NumScannedFiles/p0*v032.WDS_AV_NumScannedFiles.rrd "1day" "AVERAGE"  "Number of Files Scanned" ""
	printrrd  WDS_AV_NumScannedFiles/p0*v032.WDS_AV_NumScannedFiles.rrd "1week" "AVERAGE"  "Number of Files Scanned" ""
	printrrd  WDS_AV_NumScannedFiles/p0*v032.WDS_AV_NumScannedFiles.rrd "1month" "AVERAGE"  "Number of Files Scanned" ""
	printrrd  WDS_AV_NumScannedFiles/p0*v032.WDS_AV_NumScannedFiles.rrd "1year" "AVERAGE"  "Number of Files Scanned" ""
	#########################HTTP Requests Per Second
	printrrd  WDS_Squid_HTTPRequestsPerSec/*.WDS_Squid_HTTPRequestsPerSec.rrd "4hours" "AVERAGE"  "HTTP Requests from clients" "per sec"
	printrrd  WDS_Squid_HTTPRequestsPerSec/*.WDS_Squid_HTTPRequestsPerSec.rrd "1day" "AVERAGE"  "HTTP Requests from clients" "per sec"
	printrrd  WDS_Squid_HTTPRequestsPerSec/*.WDS_Squid_HTTPRequestsPerSec.rrd "1week" "AVERAGE"  "HTTP Requests from clients" "per sec"
	printrrd  WDS_Squid_HTTPRequestsPerSec/*.WDS_Squid_HTTPRequestsPerSec.rrd "1month" "AVERAGE"  "HTTP Requests from clients" "per sec"
	printrrd  WDS_Squid_HTTPRequestsPerSec/*.WDS_Squid_HTTPRequestsPerSec.rrd "1year" "AVERAGE"  "HTTP Requests from clients" "per sec"

	#########################HTTP request traffic KB in from clients
	printrrd  WDS_Squid_KBFromPerSec/*.WDS_Squid_KBFromPerSec.rrd "4hours" "AVERAGE"  "HTTP Requests from clients in KB" "per sec"
	printrrd  WDS_Squid_KBFromPerSec/*.WDS_Squid_KBFromPerSec.rrd "1day" "AVERAGE"  "HTTP Requests from clients in KB" "per sec"
	printrrd  WDS_Squid_KBFromPerSec/*.WDS_Squid_KBFromPerSec.rrd "1week" "AVERAGE"  "HTTP Requests from clients in KB" "per sec"
	printrrd  WDS_Squid_KBFromPerSec/*.WDS_Squid_KBFromPerSec.rrd "1month" "AVERAGE"  "HTTP Requests from clients in KB" "per sec"
	printrrd  WDS_Squid_KBFromPerSec/*.WDS_Squid_KBFromPerSec.rrd "1year" "AVERAGE"  "HTTP Requests from clients in KB" "per sec"

	#########################HTTP reply traffic KB back to clients 
	printrrd  WDS_Squid_KBToPerSec/*.WDS_Squid_KBToPerSec.rrd "4hours" "AVERAGE"  "HTTP Replies to clients in KB" "per sec"
	printrrd  WDS_Squid_KBToPerSec/*.WDS_Squid_KBToPerSec.rrd "1day" "AVERAGE"  "HTTP Replies to clients in KB" "per sec"
	printrrd  WDS_Squid_KBToPerSec/*.WDS_Squid_KBToPerSec.rrd "1week" "AVERAGE"  "HTTP Replies to clients in KB" "per sec"
	printrrd  WDS_Squid_KBToPerSec/*.WDS_Squid_KBToPerSec.rrd "1month" "AVERAGE"  "HTTP Replies to clients in KB" "per sec"
	printrrd  WDS_Squid_KBToPerSec/*.WDS_Squid_KBToPerSec.rrd "1year" "AVERAGE"  "HTTP Replies to clients in KB" "per sec"

	#########################Squid's service Time from request to reply
	printrrd  WDS_Squid_ServiceTime/*.WDS_Squid_ServiceTime.rrd "4hours" "AVERAGE"  "Squid Service Time" "in sec"
	printrrd  WDS_Squid_ServiceTime/*.WDS_Squid_ServiceTime.rrd "1day" "AVERAGE"  "Squid Service Time" "in sec"
	printrrd  WDS_Squid_ServiceTime/*.WDS_Squid_ServiceTime.rrd "1week" "AVERAGE"  "Squid Service Time" "in sec"
	printrrd  WDS_Squid_ServiceTime/*.WDS_Squid_ServiceTime.rrd "1month" "AVERAGE"  "Squid Service Time" "in sec"
	printrrd  WDS_Squid_ServiceTime/*.WDS_Squid_ServiceTime.rrd "1year" "AVERAGE"  "Squid Service Time" "in sec"

	# Lastly, print service times
	/usr/local/bin/opsadmin/PrintExceededServiceTimes.sh `echo $month | awk -F- '{print $1}'` `echo $month | awk -F- '{print $2}'` 
fi

echo
echo "============================================================="
echo "End of report"
