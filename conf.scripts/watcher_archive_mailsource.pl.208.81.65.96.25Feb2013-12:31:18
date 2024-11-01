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
our $VERSION    = 1.1;
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
my $region = ();
my $config = ("/usr/local/bin/opsadmin/watcher/watcher_archive.xml");
my $dbname = ("watcher");

##
## Create variables for CID processing
##
#
# Create the hashes for CIDs
#
my %timestamp = ();              # epoch seconds
my %solrips = ();
my %cstnam = ();

#
# Create hashes for SIDs
#
my %ms_timestamp = ();
my %ms_status = ();
my %ms_host = ();
my %ms_user = ();
my %ms_port = ();
my %ms_type = ();
my %ms_name = ();
my %ms_desc = ();
my %ms_backlog = ();
my %ms_ingest24 = ();

#
# Non hashes
#
my $policydb = ();
my $archivedb = ();
my $results = ();
my $cid_counter = ();
my @cids = ();
my $ip = ();
my @mailers = ();
my $ms_ref = ();
my %ms_hash = ();
my $cmd = ();
my $insert = ();
my $dbh = ();
my $sth = ();
my $cid = ();
my @maspool = ();
my @solrips = ();

#
# Create the server specific hashes
#
my %srv_type = ();
my %datacenter = ();

#
# Non hashes for servers
#
my $srv_ip = ();
my @masips = ();
my $ts = ();
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

		print "\n\nWorking CID: getting data\n";
		print Dumper $cid;

		$cid_counter++;

		$timestamp{$cid} = time();

		if (( $flags{'debug'} ) || ( $flags{'verbose'} ))
		{
			print "\ntimestamp\n";
			print Dumper \%timestamp;
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

##
## Loop through all the mail sources for this CID
##
		@mailers = `/usr/bin/psql -At -h $archivedb -U postgres mxl_archive -c "select server_id from arc_mail_source where id = $cid"`;
      chomp(@mailers);

      foreach my $sid (@mailers) 
		{
			print "Working SID \n";
			print Dumper $sid;

         $ms_ref = Arch::ms_from_sid($sid);
         %ms_hash = %$ms_ref;

			$ms_timestamp{$sid} = time();
			chomp($ms_timestamp{$sid});

			$ms_status{$sid} = `/usr/bin/psql -At -h $archivedb -U postgres mxl_archive -c "select last_poll_status from arc_mail_source where server_id=$sid"`;
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

			$ms_type{$sid} = `/usr/bin/psql -At -h $archivedb -U postgres mxl_archive -c "select type_id from arc_mail_source where server_id=$sid"`;
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

      	$dbh = DBI->connect("dbi:Pg:dbname=$dbname;host=localhost","postgres","") || die $DBI::errstr;

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
## Put an entry to mark the last successful completion
##
	if ( ! $flags{'test'} )
	{
		my $lr_timestamp = time();

		$insert = "psql -U postgres -d $dbname -h localhost -c \"insert into arc_ms_lastrun (epochtime,thread) values (to_timestamp($lr_timestamp),$iteration)\"";

		if ( $flags{'debug'} ) 
		{			# do not populate the database when debugging
			print "\narc_ms_lastrun insert command\n";
			print Dumper $insert;
		}
		else
		{
			if ( $flags{'verbose'} )
			{
				print "\narc_ms_lastrun insert command\n";
				print Dumper $insert;
			}
			print "\narc_ms_lastrun insert \n";
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
    -s, --start	            Start subscript
    -e, --end	    	         Ending subscript
    -i, --iteration	    	   Thread number
    -h, --help                Print this help
EOF

        print $usage;
}

