#!/usr/bin/perl -w
###/mxl/var/perl/bin/perl -w
################################################################################################
#
# Script: Metrics
# By: John Vossler
# Dated: 12.26.2006
# Purpose: To extract trending metrics. Defined below.
#
# Metric			Units		Source
# Inbound MTA Load		(Unix Load)	900SnapShot
# Outbound MTA Load		(Unix Load)	900SnapShot
# Inbound Traffic		(Msg / Day)	Audit
# Quarantine Traffic		(Msg / Day)	Audit
# Outbound Traffic		(Msg / Day)	Audit
# Quarantine DB Size		(#)		Direct Querry
# SQR Reports Generated		(#)		RRD
# SQR Reports Finished		(time) 		RRD
# DB Maintenance Finished	(time)		RRD
################################################################################################

package mxlAgent;
use base 'LWP::UserAgent';
use lib '/usr/local/bin/opsadmin/perl/';

sub get_basic_credentials {
    return 'admin', 'or3g0n';
}

package main;
our $author_email	= 'John_Vossler\@McAfee.com';
our $script_name	= 'metrics.pl';
our $total_metrics	= 15;

################################################################################################
### Package Includes 

use diagnostics;
use strict;
use warnings;

use Data::Dumper;
use DBI;
use Getopt::Long;
use MIME::Base64;
use MXL::Arch;
use MXL::MXL;
use Net::SMTP;
use POSIX qw(strftime);
use Time::Local;

################################################################################################
### Constants 

# Reporting Levels.
use constant MXL_DEBUG  => 1;
use constant MXL_TIMING => 2;
use constant MXL_INFO   => 3;
use constant MXL_WARN   => 4;
use constant MXL_ERROR  => 5;
use constant MXL_FATAL  => 6;

use constant DAY_SEC	=> 86400;

################################################################################################
### Variables 

my %hshLogTypes = (
        MXL_DEBUG,  "DEBUG",
        MXL_TIMING, "TIMING",
        MXL_INFO,   "INFO",
        MXL_WARN,   "WARNING",
        MXL_ERROR,  "ERROR",
        MXL_FATAL,  "FATAL ERROR"
);

# EMail.
###my @daily_email	= ('john_vossler@mcafee.com');
my @daily_email	= ('john_vossler@mcafee.com', 'nathan_peterson@mcafee.com', 'david_bean@mcafee.com');
#my @daily_email	= ('john_vossler@mcafee.com',);
my @debug_email	= ('john_vossler@mcafee.com');

# DB Variables
my $db_port='5432';
my $db_user='postgres';
my $db_pass='dbG0d';

# Flags.
our ($debug_flag, $email_flag, $verbose_flag)			= (0,0,0);
our ($no_db_flag, $no_sqr_flag)					= (0,0);
our ($no_inbound_audit_report, $no_outbound_audit_report)	= (0,0);

# Storage.
my ($d3_mta_inbound_load, $d3_mta_outbound_load)				= (0,0);
my ($l3_mta_inbound_load, $l3_mta_outbound_load)				= (0,0);
my ($d3_inbound_mta_count, $l3_inbound_mta_count)				= (0,0);
my ($total_mta_inbound_load, $total_mta_outbound_load)				= (0,0);
my ($mta_inbound_traffic, $mta_outbound_traffic, $mta_quarantine_traffic)	= (0,0,0);
my ($quarantine_size)								= (0);
my ($sqr_reports_generated, $sqr_reports_finished_time)				= (0,0);
my ($db_maintenience_finished)							= (0);
my ($wds_cpl_length, $wds_cpl_size)						= (0, 0);

# New Stuff - Archiving.
my%arch_data;
my($arch_num_cust, $arch_num_seats)						= (0,0);
my($arch_mesgs_ingested, $arch_msgs_backloged)					= (0,0);
my($arch_data393_store_size, $arch_level3_store_size)				= (0,0);
my($arch_data393_index_size, $arch_level3_index_size)				= (0,0);
my($arch_data393_index_searches, $arch_level3_index_searches)			= (0,0);

# New Stuff - WDS
my %wds_data;
my($wds_min_latency, $wds_max_latency)						= (0,0);
my($wds_avg_latency, $wds_avg_peak_latency)					= (0,0);
my($wds_num_blocked, $wds_num_allowed)						= (0,0);
my($wds_num_viruses)								= (0);
my($wds_bw_in, $wds_bw_out)							= (0,0);

 my @results	= (0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0);
our ($pod)	= (0);
our $warnings	= "The following warnings were encountered during processing:\n";

# Path and Formats.
our $data_dir			= '/usr/local/bin/opsadmin/data/metrics.v8';
#our $daily_audit_report_dir	= '/usr/local/bin/opsadmin/data/audit_reports/';
our $daily_audit_report_format	= "%s/domain_traffic-%s-daily-pod_%01d.%02d%02d%04d.csv";
our $db_stats_log		= '/var/log/mxl/db_stats.log';
our $log_directory		= '/usr/local/bin/opsadmin/log';
our $log_file_format		= '%04d-%02d-%02d_%02d:%02d:%02d.txt';

#Time.
our $utime			= time;
my @ltime			= localtime;
my $override_time		= '';
my ($year,$month,$day)		= ($ltime[5]+1900,$ltime[4]+1,$ltime[3]);
my ($hour,$minute,$second)	= ($ltime[2],$ltime[1],$ltime[0]);

our @run_time			= ($year,$month,$day,$hour,$minute,$second);
our @smhdmy			= ($second,$minute,$hour,$day,$month,$year);
our @ymdhms			= ($year,$month,$day,$hour,$minute,$second);


################################################################################################
### Subroutine prototypes
sub check_port($$);
sub count_mtas($$);
sub check_smtp();
sub day_before(@);
sub email_daily_results(@@%%);
sub generate_db_stats_cvs_entry($);
sub get_arch_data();
sub get_log_db_maint_finished_time();
sub get_spam_report_data();
sub get_quar_db_size();
sub get_wds_cpl_data();
sub get_wds_data();
sub logwrite($$);
sub process_900snapshot($$);
sub rev_array(@);
sub querry_log_db_all_mesg_in();
sub querry_log_db_all_mesg_out();
sub querry_log_db_quar_mesg_in();
sub sci_to_int($);
sub summarize_data(@%%);
sub summarize_db_stats($);
sub usage();
sub validate_override_time($);
sub write_daily_data(@%%);
sub write_monthly_data(@%%);
sub ymdhms_human2unix(@);
sub ymdhms_unix2human(@);


################################################################################################
### Begin MAIN process.

# Process Command Line Options or die.
my $result = GetOptions (	
				"d+"		=> \$debug_flag,		# incrimental
				"debug+"	=> \$debug_flag,		# incrimental
				"e"		=> \$email_flag,		# flag
				"email"		=> \$email_flag,		# flag
				"p=i"		=> \$pod,			# numeric
				"pod=i"		=> \$pod,			# numeric
				"t=i"		=> \$override_time,		# string
				"time=i"	=> \$override_time,		# string
				"nodb"		=> \$no_db_flag,		# flag
				"nosqr"		=> \$no_sqr_flag,		# flag
				"v"		=> \$verbose_flag,		# flag
				"verbose"	=> \$verbose_flag		# flag
			);
unless($result)
{ logwrite(MXL_FATAL, "There was an error interpreting the command line options given."); }

# If this is a debugging effort, don't email the normal recipients or store data in the usual places.
if($debug_flag)
{
	@daily_email	= @debug_email;
	$log_directory	= '/usr/local/bin/opsadmin/log/metrics.debug';
	$data_dir	= '/usr/local/bin/opsadmin/data/metrics.debug'; 
}

# Open the logfile or die.
my $logfile = $log_directory."/".sprintf($log_file_format, @ymdhms);
open(LOGFILE, ">$logfile")
	or logwrite(MXL_FATAL, "$script_name could not open logfile, $logfile. Exiting.\n");
logwrite(MXL_INFO,"$script_name initialized. Running.");


# DBs.
our %dbs =	(
			"10.$pod.106.130", 'mxl',
			"10.$pod.106.131", 'mxl_log',
			"10.$pod.106.132", 'mxl_quar',
			"10.$pod.106.135", 'mxl_quar',
			"10.$pod.106.136", 'mxl_log',
		);

#Assure the pod variable is valid and welcome users to the script.
if (($pod != 1) && ($pod != 2)) { 
	&usage;
	logwrite(MXL_FATAL,"The -p flag was not used, or was used improperly. Dying."); 
} else {
	if(($debug_flag) || ($verbose_flag)) {
		print "Welcome to Metrics! (A daily metric gathering script)\n";
		print "Processing Denver Pod $pod\n";
	}

}

# Timing is everything.
# This script behaves as if it were 9AM yesterday, unless ...
# specifically overidden by command line option above.

# Check for the command line time override and process.
if($override_time) {

	my $msg = "An override time of $override_time, has been sumbitted.";
	logwrite(MXL_INFO, $msg);

	if(($debug_flag) || ($verbose_flag))
	{ print "$msg\n"; }

	@ymdhms = validate_override_time($override_time);

	$msg = "The override time has been accepted. This run is now for @ymdhms\n";
	logwrite(MXL_INFO, $msg);

	if(($debug_flag) || ($verbose_flag))
	{ print "$msg\n"; }

} else {

	@ymdhms = day_before(@ymdhms);
	@ymdhms = ($ymdhms[0], $ymdhms[1], $ymdhms[2], 9, 0, 0);

	my $msg =	"Override time was _not_ specified, so the effective time will be 9AM".
			" yesterday: ".sprintf("%04d/%02d/%02d %02d:%02d:%02d",@ymdhms). "\n";

	if(($debug_flag) || ($verbose_flag))
	{ print "$msg\n"; }
	logwrite(MXL_INFO, $msg);
}

# Reset the smhdmy and utime now that @ymdhms is officially established.
$utime=timelocal(rev_array(ymdhms_human2unix(@ymdhms)));
@smhdmy = rev_array(@ymdhms);

my $msg =	"$script_name has established it's effective run time: ".
		sprintf("%04d/%02d/%02d %02d:%02d:%02d", @ymdhms)."\n".
		"The equivalent Unix time is, $utime\n";
		"Verify that ".(strftime "%a %b %e %H:%M:%S %Y", localtime($utime)).
		" is equivalent as well\n";

logwrite(MXL_INFO, $msg);
if(($debug_flag) || ($verbose_flag)) {
	print $msg;
}


if($debug_flag){ print "Checkpoint 1\n"; }
################################################################################################
# Inbound Traffic		(Msg / Day)	Audit	Stored in: $mta_inbound_traffic 
# Quarantine Traffic		(Msg / Day)	Audit	Stored In: $mta_quarantine_traffic
# Outbound Traffic		(Msg / Day)	Audit	Stored In: $mta_outbound_traffic  
unless($no_db_flag) {
	($mta_inbound_traffic) = querry_log_db_all_mesg_in();
	($mta_quarantine_traffic) = querry_log_db_quar_mesg_in();
	($mta_outbound_traffic) = querry_log_db_all_mesg_out();
}
if($debug_flag){ print "Checkpoint 2\n"; }
### The MTA Load is already computed by shinto in the file /tmp/900snapshot.log
# Inbound MTA Load		(Unix Load)	900SnapShot	Stored In: $mta_inbound_load
# Outbound MTA Load		(Unix Load)	900SnapShot	Stored In: $mta_outbound_load
($d3_mta_inbound_load, $d3_mta_outbound_load) = process_900snapshot('localhost', '/tmp/900snapshot.log');

if($debug_flag){ print "Checkpoint 3\n"; }
### The MTA Load is already computed by shinto in the file /tmp/900snapshot.log
### The MTA Load is already computed by shinto in the file /tmp/900snapshot.log
# Inbound MTA Load		(Unix Load)	900SnapShot	Stored In: $mta_inbound_load
# Outbound MTA Load		(Unix Load)	900SnapShot	Stored In: $mta_outbound_load
($l3_mta_inbound_load, $l3_mta_outbound_load) = process_900snapshot("10.$pod.107.1", '/tmp/900snapshot.log');

if($debug_flag){ print "Checkpoint 4\n"; }
### The MTA Load is already computed by shinto in the file /tmp/900snapshot.log
### The number of MTAs in production at any given point in time is somewhat variable.
# Number of Inbound MTAs at D393:	$d3_inbound_mta_count
# Number of Inbound MTAs at L3:		$l3_inbound_mta_count
($d3_inbound_mta_count) = count_mtas($pod, 106);
if($debug_flag){ print "Checkpoint 4.5\n"; }
($l3_inbound_mta_count) = count_mtas($pod, 107);

if($debug_flag){ print "Checkpoint 5\n"; }
### The MTA Load is already computed by shinto in the file /tmp/900snapshot.log
# The totals are simply the sums.
($total_mta_inbound_load, $total_mta_outbound_load) =	(($d3_mta_inbound_load+$l3_mta_inbound_load),($d3_mta_outbound_load+$l3_mta_outbound_load));

if($debug_flag){ print "Checkpoint 6\n"; }
### The MTA Load is already computed by shinto in the file /tmp/900snapshot.log
### Directly querry the quarantine DB for the number of items in quarantine. ###
# Quarantine DB Size		(#)		RRD	Stored In: $quarantine_size
unless($no_db_flag) {
	$quarantine_size = get_quar_db_size();
}

if($debug_flag){ print "Checkpoint 7\n"; }
### The MTA Load is already computed by shinto in the file /tmp/900snapshot.log
# Gather the Spam (SQR) Reports' data from RRD records.
# SQR Reports Generated		(#)		RRD	Stored In: $sqr_reports_generated
# SQR Reports Finished		(time) 		RRD	Stored In: $sqr_report_finished_time
unless($no_sqr_flag) {
	($sqr_reports_generated, $sqr_reports_finished_time) = get_spam_report_data();
}

if($debug_flag){ print "Checkpoint 8\n"; }
### The MTA Load is already computed by shinto in the file /tmp/900snapshot.log
# Gather the DB maintenance data from RRD records.
# DB Maintenance Finished	(time)		RRD	Stored In: $db_maintenience_finished
$db_maintenience_finished = get_log_db_maint_finished_time();

if($debug_flag){ print "Checkpoint 9\n"; }
### The MTA Load is already computed by shinto in the file /tmp/900snapshot.log
# Get WDS CPl data 
($wds_cpl_length, $wds_cpl_size)  = get_wds_cpl_data();

################################################################################################

# Consolidate the results in an array.
@results =	(
			$l3_mta_inbound_load,		# 1
			$l3_mta_outbound_load,		# 2
			$l3_inbound_mta_count,		# 3
			$d3_mta_inbound_load,		# 4
			$d3_mta_outbound_load,		# 5
			$d3_inbound_mta_count,		# 6
			$total_mta_inbound_load,	# 7
			$total_mta_outbound_load,	# 8
			$mta_inbound_traffic,		# 9
			$mta_quarantine_traffic,	#10
			$mta_outbound_traffic,		#11
			$quarantine_size,		#12
			$sqr_reports_generated,		#13
			$sqr_reports_finished_time,	#14
			$db_maintenience_finished,	#15
			$wds_cpl_length,		#16
			$wds_cpl_size			#17
		);

%arch_data = &get_arch_data();
%wds_data = &get_wds_data();

# Write the resulting data to the daily data file.
&write_daily_data(\@results, \%arch_data, \%wds_data); 

# Write the resulting data to the monthly data file.
&write_monthly_data(\@results, \%arch_data, \%wds_data); 

# Send daily email reports.
if($email_flag) {
	&email_daily_results(\@results, \@daily_email, \%arch_data, \%wds_data);
}

if(($debug_flag) || ($verbose_flag)) {
	print "\n",&summarize_data(\@results, \%arch_data, \%wds_data),"\n";
	logwrite(MXL_INFO,	"The following were this run\'s results:\n\n".
				&summarize_data(\@results, \%arch_data, \%wds_data)."\n");
}

logwrite(MXL_INFO,"$script_name is finished. Closing.");
close LOGFILE;

### End MAIN processing. 
################################################################################################



################################################################################################
### Subroutines

# Subroutine:   check_port
# Args:         $ip (sting), $port (string)
# Return Value: true / false (is open?)
# Purpose:      Given an IP and port, return a boolean (true/false)
#		of whether or not this port is open.
sub check_port($$)
{
	my ($ip, $port) = @_;
	my $sock =	new IO::Socket::INET(	PeerAddr => $ip,
						PeerPort => $port,
						Proto => 'tcp',
						Timeout=>2
						#) || warn "What's up with the sock?\n$!\n";
						);
        if ($sock) {
		return 1;
        } else {
		#print "No connection allowed to $ip on $port\n";
		return 0;
        }

	undef $sock;
}

# Subroutine:   count_inbound_mtas
# Args:         $pod (string), $data_center (string)
# Return Value: $count 
# Purpose:      Given a data center, return the number of operational MTAs (inbound).
sub count_mtas($$)
{
        my ($pod, $dc) = @_;
	my $count = 0;

	my @mtas = `ssh 10.$pod.$dc.1 'cat /root/dist/mail.inbound'`;
	#print "ssh 10.$pod.$dc.1 'cat /root/dist/mail.inbound'\n";

	foreach my $mta (@mtas) {
		chomp($mta);
		#print "$mta\n";
		if(&check_port($mta, '25')) {
			$count++;
		}
	}

	#print "MTA Count: $count\n";
	return $count;
}

# Subroutine:	day_before
# Args:		@ymdhms(array)
# Return Value:	@ymdhms(array)
# Purpose:	Given a ymdhms array, return the same for one day before. 
sub day_before(@)
{
	my $subroutine_name = 'day_before';
	my ($year, $month, $day, $hour, $minute, $second) = @_;

	my @temp = ($year, $month, $day, $hour, $minute, $second);
	my @pmet = ();

	foreach my $entry (@ymdhms) {
                push @pmet, pop @temp;
        }
	$pmet[4] = $pmet[4]-1;

	my $time = timelocal(@pmet);
	my $yesterday = $time - DAY_SEC;

	my @new_ltime = localtime($yesterday);
	my @return_array = (	$new_ltime[5]+1900,$new_ltime[4]+1,$new_ltime[3],
				$new_ltime[2],$new_ltime[1],$new_ltime[0]);

	return @return_array;
}

# Subroutine:	email_daily_results
# Args:		@data (array), @recipients (array)
# Return Value:	<void>
# Purpose:	Email recipients with the daily summary of Metrics. 
sub email_daily_results(@@%%)
{

	my $subroutine_name = 'email_daily_results';

	if($debug_flag)
	{ print "DEBUG: Entering $subroutine_name.\n"; }
	elsif($verbose_flag)
	{ print "Emailing daily metrics to the recipient list.\n"; }
	else
	{ ; }

	my ($ref1, $ref2, $ref3, $ref4)	= @_;

	my @data	= @$ref1;
	my @recipients	= @$ref2;
	my %arch_data	= %$ref3;
	my %wds_data	= %$ref4;

	my $smtp = Net::SMTP->new("127.0.0.1", Port=>"25")
			|| logwrite(MXL_FATAL,"Subroutine: email_daily_results: could not initiate the smtp session.");

	my $attach = get_monthly_data($ymdhms[0], $ymdhms[1]);

	my $content = 
                "From: Metrics <metrics\@mcafee.com>\n" .
                "To: Unspecifcied Recipients <john_vossler\@mcafee.com>\n" .
                "Subject: Metrics: Daily Output: ".sprintf("%04d-%02d-%02d", @ymdhms).": Denver Pod ".$pod."\n" .
                "MIME-Version: 1.0\n" .
                "Content-Transfer-Encoding: 7bit\n" .
		"Content-Type: multipart/mixed;\n" .
                "\tboundary=\"____BOUNDARY____\"\n" .
                "\n" .
                "This is a multi-part message in MIME format.\n" .
                "\n" .
                "--____BOUNDARY____\n" .
                "Content-Type: text/plain; charset=\"iso-8859-1\"\n" .
                "\n" .
		&summarize_data(\@data, \%arch_data, \%wds_data).
		"\n".$warnings."\n".
                "\n\nMonthly summary data attached.\n" .
                "--____BOUNDARY____\n" .
                "Content-Type: text/plain;\n" .
                "\tName=\"".sprintf("%04d-%02.csv",@ymdhms)."\"\n" .
                "Content-Transfer-Encoding: base64\n" .
                "Content-Disposition: attachment;\n" .
                "\tfilename=\"".sprintf("%04d-%02d.csv",@ymdhms)."\"\n" .
                "\n" .
                encode_base64($attach) . "\n" .
                "--____BOUNDARY____\n";

	if($debug_flag){
		print "\tSubroutine: email_daily_results: I am about to email :\n";
		foreach my $recipient (@recipients) {
			print "\t\t$recipient\n";
		}
		if($debug_flag > 1)
		{ print "\tWith the following content:\n$content\n"; }
	}
	foreach my $recipient (@recipients) {
		logwrite(MXL_INFO, "A daily report was emailed to $recipient.");
	}

	$smtp->mail("metrics\@mcafee.com")
		or logwrite(MXL_FATAL, "Subroutine: email_daily_results: SMTP from address rejected.\n".$smtp->code."\n".$smtp->message);
	$smtp->recipient(@recipients, { Notify => ['NEVER'], SkipBad => 1 })
		or logwrite(MXL_FATAL, "Subroutine: email_daily_results: SMTP recipients rejected\n".$smtp->code."\n".$smtp->message);
	$smtp->data()
		or logwrite(MXL_FATAL, "Subroutine: email_daily_results: SMTP host denied data.\n".$smtp->code."\n".$smtp->message);
	$smtp->datasend($content) 
		or logwrite(MXL_FATAL, "Subroutine: email_daily_results: SMTP did not accept the content.\n".$smtp->code."\n".$smtp->message);
	$smtp->dataend()
		or logwrite(MXL_FATAL, "Subroutine: email_daily_results: SMTP rejected message.\n".$smtp->code."\n".$smtp->message);
        $smtp->quit()
		or logwrite(MXL_FATAL, "Subroutine: email_daily_results: SMTP did not end properly.\n".$smtp->code."\n".$smtp->message);
}

# Subroutine:	generate_db_stats_cvs_entry($)
# Args:		$db (scalar)
# Return Value: $cvs_entry (scalar)
# Purpose:      For each Log DB, grab DB maintenence info, average the time and return.
sub generate_db_stats_cvs_entry($) {

	my $subroutine_name = 'generate_db_stats_cvs_entry';

	if($debug_flag)
	{ print "DEBUG: Entering $subroutine_name.\n"; }

	my ($db) = @_;

	my @data = ();

	my $date_str = sprintf('%02d/%02d/%04d',$ymdhms[1],$ymdhms[2],$ymdhms[0]);

	my $expected_data_points = 12;
	my $found = 0;

	open(DB_STATS, "<$db_stats_log");
	my @lines = <DB_STATS>;
	close DB_STATS;

	foreach my $line (@lines) {
		if(($line =~ $db) && ($line =~ $date_str)) {
			chomp($line);
			@data = split(/,/,$line,$expected_data_points);
			if($debug_flag) {
				print "Subroutine: $subroutine_name - Found line in $db_stats_log for $db on $date_str.\n";
				print "Subroutine: $subroutine_name - $line\n";
				print "Subroutine: $subroutine_name - ".join(',', @data)."\n";
			}	

			$found = 1;
		}

	}

	my $return_string = '';
	unless($found) {
		$return_string = "$dbs{$db},$db,NaN,NaN,NaN,NaN,NaN,NaN,NaN,NaN,NaN";	
		return $return_string;
	}
	
	for(my $ii=0; $ii<$expected_data_points;$ii++) {
		if($data[$ii] eq '') { $data[$ii] = 'NaN'; }
	}

	unless(($data[0] eq $date_str) && ($data[2] eq $db) && ($data[1] eq $dbs{$db})) {
		logwrite(MXL_FATAL, "Subroutine: $subroutine_name: Data from file $db_stats_log does not match what is expected.\n");
	}
	
	shift @data;

	$return_string = join(',', @data);

	if($debug_flag) {
		print "DEBUG: $subroutine_name: returning $return_string\n";
	}

	return $return_string;
}

# Subroutine:   get_arch_data
# Args		none
# Return Value: %arch_data (hash)
# Purpose:      Responsible for collecting WDS data and returning it in a hash structure.
sub get_arch_data()
{
#	my($arch_num_cust, $arch_num_seats)                                             = (0,0);
#	my($arch_mesgs_ingested, $arch_msgs_backloged)                                  = (0,0);
#	my($arch_data393_store_size, $arch_level3_store_size)                           = (0,0);
#	my($arch_data393_index_size, $arch_level3_index_size)                           = (0,0);
#	my($arch_data393_index_searches, $arch_level3_index_searches)                   = (0,0);

	my %data = (
		num_cust => '-1',
		num_seats => '-1',
		msgs_ingested => '-1',
		msgs_backlogged => '-1',
		data393_store_size => '-1',
		level3_store_size => -1,
		data393_index_size => -1,
		level3_index_size => -1,
		data393_searches => -1,
		level3_searches => -1,
	);
	my $line = '';
	my $log_file = '';
	my @parts = {};

	# Count Archiving Customers.
	my @cids = Arch::cids_from_mail_source();
	$data{'num_cust'} = $#cids;
	
	#Count Archiving Seats.
	my $seat_count = 0;

	foreach my $cid (@cids) {
		my $seats =  int(MXL::customer_seats_from_cid($cid));
		$seat_count = $seat_count + $seats;
	}

	$data{'num_seats'} = $seat_count;

	# Count Messages Archived.
	my $ingest_count = 0;

	foreach my $cid (@cids) {
		my $count = int(`/usr/local/bin/opsadmin/arc_cust_vel.pl --cid $cid`);
		#print "Count: $count\n";
		$ingest_count = $ingest_count + $count;
	}

	$data{'msgs_ingested'} = $ingest_count;
	
	# Count Messages Backlogged.
	$data{'msgs_backlogged'} = `sqlite3 /tmp/arc.cust.data.db 'select sum(count) from backlog ;'`;
	
	# Get Data393 Store Size
	$line = `ssh 10.$pod.106.54 'df /mxl/msg_archive/mas/mnt/data393 -P' | tail -1`;
	@parts = split(/\s+/, $line);
	$data{'data393_store_size'} = $parts[2];

	# Get Level3 Store Size
	$line = `ssh 10.$pod.106.54 'df /mxl/msg_archive/mas/mnt/level3 -P' | tail -1`;
	@parts = split(/\s+/, $line);
	$data{'level3_store_size'} = $parts[2];

	# Get Data393 Index Size
	$line = `ssh 10.$pod.106.137 'df /var/msg_archive -P | tail -1'`;
	@parts = split(/\s+/, $line);
	$data{'data393_index_size'} = $parts[2];

	# Get Level3 Index Size
	$line = `ssh 10.$pod.107.137 'df /var/msg_archive -P | tail -1'`;
	@parts = split(/\s+/, $line);
	$data{'level3_index_size'} = $parts[2];

	# Get Data393 Index Searches
	$log_file = sprintf('/mxl/var/jetty/logs/%04d_%02d_%02d.request.log', $ymdhms[0], $ymdhms[1], $ymdhms[2]);
	$line = `ssh 10.$pod.106.137 'wc -l $log_file'`;
	@parts = split(/\s+/, $line);
	$data{'data393_searches'} = $parts[0];

	# Get Level3 Index Searches
	$log_file = sprintf('/mxl/var/jetty/logs/%04d_%02d_%02d.request.log', $ymdhms[0], $ymdhms[1], $ymdhms[2]);
	$line = `ssh 10.$pod.107.137 'wc -l $log_file'`;
	@parts = split(/\s+/, $line);
	$data{'level3_searches'} = $parts[0];

	if($debug_flag){ print Dumper(%data); }
	
	return %data;

}

# Subroutine:   get_log_db_maint_finished_time
# Args:		<none>
# Return Value: $data_string(scalar)
# Purpose:      For each Log DB, grab DB maintenence info, average the time and return.
sub get_log_db_maint_finished_time()
{
        my $subroutine_name = 'get_log_db_maint_finished_time';

	my $cmd_format	= 'ssh %s "zcat /var/log/mxl/rotate/db_maint_mxl_log.log.*.gz; cat /var/log/mxl/db_maint_mxl_log.log"';
	my @log_dbs	= ( sprintf("10.%d.106.131", $pod), sprintf("10.%d.106.136", $pod) );
	my %maint_times	= ();
	my $today	= sprintf("%04d%02d%02d", @ymdhms);
	
	foreach my $log_db (@log_dbs) {

		my $cmd = sprintf($cmd_format, $log_db);
		my @results = `$cmd`;

		unless (@results){
			my $warning = "Subroutine: $subroutine_name: I have no results for Log DB $log_db\n";	
			logwrite(MXL_WARN, $warning);
			warn $warning;
			$warnings .= $warning;
		}

		foreach my $result (@results) {
			if(($result =~ /$today/) && ($result =~ 'Finish'))
			{ $maint_times{$log_db}=substr($result,9,8); print "Maint Time: $maint_times{$log_db}\n"; }
		} 
	}
	
	my $max = 0;

	foreach my $key ( keys %maint_times ) {
		my $tmp = $maint_times{$key};
		$tmp =~ s/://g;

		if($tmp > $max) 
		{ $max = $tmp; }
        }

	my $str = 'NaN';

	if($max > 0)  
	{ $str = sprintf("%02d:%02d:%02d", substr($max,0,2), substr($max,2,2), substr($max,4,2)); }

	return $str;
}

# Subroutine:   get_monthly_data
# Args:		$year(scalar), $month(scalar)
# Return Value: $data_string(scalar)
# Purpose:      Grab the monthly data file's contents and chunk.
sub get_monthly_data($$)
{
        my $subroutine_name = 'get_monthly_data';

	my ($year, $month) = @_;

        open(MONTHLY, "<$data_dir/monthly/".sprintf("%04d-%02d.csv", $year, $month))
                or logwrite(MXL_FATAL, "Subroutine: $subroutine_name: Could not open monthly report.\n");
        my @monthly = <MONTHLY>;
        close MONTHLY;

        my $data_string = '';
        foreach my $line (@monthly)
        { $data_string .= $line; }

	return $data_string;
}


# Subroutine:   get_spam_report_data
# Args:		<none>
# Return Value:	$sqr_reports_generated (scalar), $sqr_reports_finished_time (scalar)
# Purpose:      For each Spam Quarantine Report (SQR) machine, grab info on number of reports generated and time completed.
sub get_spam_report_data()
{
        my $subroutine_name = 'get_spam_report_data';

        my $count_cmd_format    = 'ssh %s "zgrep Reports /var/log/mxl/rotate/spam_report.log*gz; grep Reports /var/log/mxl/spam_report.log*"';
        my $time_cmd_format     = 'ssh %s "zgrep \"*** End run ***\" /var/log/mxl/rotate/spam_report.log*gz;'.
				  ' grep \"*** End run ***\" /var/log/mxl/spam_report.log*"';

	my %sqr_report_times	= ();
	my %sqr_report_counts	= ();

	my @sqr_boxen	= ( sprintf("10.%d.106.61", $pod), sprintf("10.%d.106.62", $pod), sprintf("10.%d.106.63", $pod) );

	my $today	= strftime "%a %b %e ", localtime($utime);
	
	foreach my $sqr_box (@sqr_boxen) {

		my $count_cmd = sprintf($count_cmd_format, $sqr_box);
		my @count_results = `$count_cmd`;

		my $time_cmd = sprintf($time_cmd_format, $sqr_box);
		my @time_results = `$time_cmd`;

		unless ((@count_results) && (@time_results)){
			my $warning = "Subroutine: $subroutine_name: I have no results for SQR box, $sqr_box.\n";	
			logwrite(MXL_WARN, $warning);
			warn $warning;
			$warnings .= $warning;
			next;
		}

		foreach my $count_result (@count_results) {
			chomp ($count_result);
			if($count_result =~ /$today/) {
				
				my @parts = split(/ /, $count_result);
				
				$sqr_report_counts{$sqr_box} = $parts[$#parts];	
			}
		} 
	
		foreach my $time_result (@time_results) {
			chomp ($time_result);
			if($time_result =~ /$today/) {

				my @parts1 = split(/\[/, $time_result);
				my @parts2 = split(/\s+/,$parts1[1]);

				$sqr_report_times{$sqr_box} = $parts2[3];
			}
		} 
	}
	
	my $count = 0;
	my $finish_time;
	my $max = 0;

	foreach my $key ( keys %sqr_report_times ) {
		my $tmp = $sqr_report_times{$key};
		$tmp =~ s/://g;
	
		if($tmp > $max) {
			$max = $tmp;
			#$finish_time = sprintf("%04d:%02d:%02d:%s", $ymdhms[0], $ymdhms[1], $ymdhms[2], $sqr_report_times{$key});
			$finish_time = $sqr_report_times{$key};
		}
	}

	$count += $_ for values %sqr_report_counts;

	unless(($max > 0) && ($count > 0))
	{ $max = 'NaN'; $count = 'NaN'; }
	
	return ($count, $finish_time);
}


# Subroutine:   get_quar_db_size
# Args:
# Return Value: $quarantine_size(scalar)
# Purpose:      Email recipients with the daily summary of Metrics.
sub get_quar_db_size()
{
        my $subroutine_name = 'get_quar_db_size';

        if($debug_flag)
        { print "DEBUG: Entering $subroutine_name.\n"; }

	my $return_val	= 'NaN';
	
	my $quar_table_format	= 'mxl_quar_%04d%02d%02d';
	my $quar_db_ip_format	= '10.%d.106.%d';
	my @quar_db_suffixes	= ('132','135');

	my $db_user = 'postgres';
	my $db_pass = 'dbG0d';

	foreach my $suffix (@quar_db_suffixes) {
		my $db_ip	= sprintf($quar_db_ip_format, $pod, $suffix);	
		my $db_source	= "dbi:Pg:dbname=mxl_quar;host=$db_ip;port=5432";
		my $db_table	= sprintf($quar_table_format, @ymdhms); 
		my $db_querry	= "SELECT COUNT(*) from $db_table";
		if(($debug_flag) || ($verbose_flag))
		{ print "Now querrying $db_ip for quaratine size. This takes several minutes to complete.\n"; }
		if($debug_flag){ 
			print "\tSubroutine:$subroutine_name: Connecting to Quar DB at $db_ip\n";
			print "\tSubroutine:$subroutine_name: DB Login w/ User: $db_user, Pass: $db_pass\n";
			print "\tSubroutine:$subroutine_name: Looking for table: $db_table\n";
			print "\tSubroutine:$subroutine_name: DB Source String: $db_source\n";
			print "\tSubroutine:$subroutine_name: Sumbitting Querry: $db_querry\n";
		}

		my $dbh = DBI->connect("$db_source", "$db_user", "$db_pass", { PrintError => 1, RaiseError=> 0, AutoCommit => 0 })
                        or logwrite(MXL_FATAL, "Subroutine:$subroutine_name: Could not connect to DB, $DBI::errstr");
	        my $sth = $dbh->prepare("$db_querry")
			or logwrite(MXL_WARN, "Subroutine:$subroutine_name: Could not prepare DB querry, $db_querry. $DBI::errstr");	
		$sth->execute()
			or logwrite(MXL_WARN, "Subroutine:$subroutine_name: Could not execute DB querry, $db_querry. $DBI::errstr");	
		my @row = $sth->fetchrow_array
			or logwrite(MXL_WARN, "Subroutine:$subroutine_name: Could fetch the return value. $DBI::errstr");	

		if($return_val eq 'NaN')
		{ $return_val = $row[0]; }
		else
		{ $return_val = $return_val + $row[0]; }

		$sth->finish;
		$dbh->disconnect;
	}
	return $return_val;
}

sub get_wds_cpl_data()
{
	my ($length, $size) = ('NaN', 'NaN');

	my $cpl_url = "https://10.$pod.64.1:8082/Policy/Current";
	print "$cpl_url\n";
	
	my  $ua = mxlAgent->new
		|| die "Could not create a new user agent.\n$!\n";
	$ua->agent("MX Logic Metrics Spider 1.0 ");

	# Create a request object.
	#my $req = HTTP::Request->new( GET => $cpl_url )
	my $res = $ua->get($cpl_url)
		|| die "Could not create request object.\n$!\n";

	# Pass request to the user agent and get a response.
	#my $res = $ua->request($req)
	#	| die "Our user agent could not execute the page request.\n".$ua->status_line."\n$!\n";

	# Check the outcome of the response
	if ($res->is_success) {

		my $save_file = "/tmp/cpl.html";

    		open OUTFILE, ">$save_file";
   	 	print OUTFILE $res->content;
    		close OUTFILE;

		my @lines = split(/\n/, $res->content);
		my @stats = stat($save_file);

		$length = $#lines;
		$size = $stats[7];

	} else {
		print $res->status_line, "\n";
	}

	return ($length, $size);
}

# Subroutine:   get_wds_data
# Args		none
# Return Value: %wds_data (hash)
# Purpose:      Responsible for collecting WDS data and returning it in a hash structure.
sub get_wds_data()
{
        my ($length, $size) = ('NaN', 'NaN');

#       my($wds_min_latency, $wds_max_latency)                                          = (0,0);
#       my($wds_avg_latency, $wds_avg_peak_latency)                                     = (0,0);
#       my($wds_num_blocked, $wds_num_allowed)                                          = (0,0);
#       my($wds_num_viruses)                                                            = (0);
#       my($wds_bw_in, $wds_bw_out)                                                     = (0,0);

        my %data = (
                min_latency => '0',
                min_between_all => '9999',
                max_latency => '0',
                max_between_all => '0',
                avg_latency => '0',
                avg_peak_latency => '0',
                reqs_allowed => 0,
                reqs_blocked => 0,
                viruses => 0,
                bw_in => 0,
                bw_out => 0,
        );

        my @proxies = (
                        "p01c64y064",
                        "p01c64y065",
                        "p01c64y066",
                        "p01c64y067",
                        #"10.$pod.65.64",
                        #"10.$pod.65.65",
                        #"10.$pod.65.66",
                        #"10.$pod.65.67",
        );

        my $count = 0;

        # Min_Latency
        $count = 0;
        my $min_between_all = 99999;
        foreach my $proxy (@proxies) {
                $count++;
                my $tmp = `/usr/local/rrdtool/bin/rrdtool graph /tmp/rmme.gif --start=-1day DEF:MyDef=/usr/local/rrdtool/nagios-trending/rrds/WDS_Squid_ServiceTime/$proxy.WDS_Squid_ServiceTime.rrd:MedianClientService:MIN CDEF:usage=MyDef PRINT:usage:MIN:%-10.6lf | tail -1`;
                chomp($tmp);

                $data{'min_latency'} = $data{'min_latency'} + $tmp;
                if ($tmp < $min_between_all) {$min_between_all = $tmp};
        }
        $data{'min_latency'} = $data{'min_latency'} / $count;
        $data{'min_between_all'} = $min_between_all;
        chomp($data{'min_latency'});

        # Max_Latency
        $count = 0;
        my $max_between_all = 0;
        foreach my $proxy (@proxies) {
                $count++;
                my $tmp = `/usr/local/rrdtool/bin/rrdtool graph /tmp/rmme.gif --start=-1day DEF:MyDef=/usr/local/rrdtool/nagios-trending/rrds/WDS_Squid_ServiceTime/$proxy.WDS_Squid_ServiceTime.rrd:MedianClientService:MAX CDEF:usage=MyDef PRINT:usage:MAX:%-10.6lf | tail -1`;
                chomp($tmp);

                $data{'max_latency'} = $data{'max_latency'} + $tmp;
                if ($tmp > $max_between_all) {$max_between_all = $tmp;}
        }
        $data{'max_latency'} = $data{'max_latency'} / $count;
        $data{'max_between_all'} = $max_between_all;
        chomp($data{'max_latency'});

        # AVG Latency
        $count = 0;
        foreach my $proxy (@proxies) {
                $count++;
                my $tmp = `/usr/local/rrdtool/bin/rrdtool graph /tmp/rmme.gif --start=-1day DEF:MyDef=/usr/local/rrdtool/nagios-trending/rrds/WDS_Squid_ServiceTime/$proxy.WDS_Squid_ServiceTime.rrd:MedianClientService:AVERAGE CDEF:usage=MyDef PRINT:usage:AVERAGE:%-10.6lf | tail -1`;
                chomp($tmp);

                $data{'avg_latency'} = $data{'avg_latency'} + $tmp;
        }
        $data{'avg_latency'} = $data{'avg_latency'} / $count;
        chomp($data{'avg_latency'});

        # AVG Peak Latency
        $count = 0;
        foreach my $proxy (@proxies) {
                $count++;
                my $tmp = `/usr/local/rrdtool/bin/rrdtool graph /tmp/rmme.gif --start=-1day DEF:MyDef=/usr/local/rrdtool/nagios-trending/rrds/WDS_Squid_ServiceTime/$proxy.WDS_Squid_ServiceTime.rrd:MedianClientService:AVERAGE CDEF:usage=MyDef PRINT:usage:AVERAGE:%-10.6lf | tail -1`;
                chomp($tmp);

                $data{'avg_peak_latency'} = $data{'avg_peak_latency'} + $tmp;
        }
        $data{'avg_peak_latency'} = $data{'avg_peak_latency'} / $count;
        chomp($data{'avg_peak_latency'});

        # Viruses Caught
        $data{'viruses'} = `/usr/local/rrdtool/bin/rrdtool graph /tmp/rmme.gif --start=-1day DEF:MyDef=/usr/local/rrdtool/nagios-trending/rrds/WDS_AV_NumVirusesCaught/p01c64v032.WDS_AV_NumVirusesCaught.rrd:VirusesCaught:LAST CDEF:usage=MyDef PRINT:usage:LAST:%-5.3lf | tail -1`;
        chomp($data{'viruses'});

        # Bandwitdh In
        $count = 0;
        foreach my $proxy (@proxies) {
                $count++;
                my $tmp = `/usr/local/rrdtool/bin/rrdtool graph /tmp/rmme.gif --start=-1day DEF:MyDef=/usr/local/rrdtool/nagios-trending/rrds/WDS_Squid_HTTPRequestsPerSec/$proxy.WDS_Squid_HTTPRequestsPerSec.rrd:NumHTTPClientReques:AVERAGE CDEF:usage=MyDef PRINT:usage:AVERAGE:%-10.6lf | tail -1`;
                chomp($tmp);

                $data{'bw_in'} = $data{'bw_in'} + $tmp;
        }
        $data{'bw_in'} = $data{'bw_in'} / $count;

        # Bandwitdh Out
        $count = 0;
        foreach my $proxy (@proxies) {
                $count++;
                my $tmp = `/usr/local/rrdtool/bin/rrdtool graph /tmp/rmme.gif --start=-1day DEF:MyDef=/usr/local/rrdtool/nagios-trending/rrds/WDS_Squid_KBToPerSec/$proxy.WDS_Squid_KBToPerSec.rrd:HTTPKBToClientsPerS:AVERAGE CDEF:usage=MyDef PRINT:usage:AVERAGE:%-10.6lf | tail -1`;
                chomp($tmp);

                $data{'bw_out'} = $data{'bw_out'} + $tmp;
        }
        $data{'bw_out'} = $data{'bw_out'} / $count;

        # Requests Allowed
        $data{'reqs_allowed'} = `cat /tmp/wds.traffic.report | grep WDSURLSOBSERVED | cut -d= -f2`;
        chomp($data{'reqs_allowed'});

        # Requests Blocked
        $data{'reqs_blocked'} = `cat /tmp/wds.traffic.report | grep WDSURLSDENIED | cut -d= -f2`;
        chomp($data{'reqs_blocked'});

        if($debug_flag){ print Dumper(%data); }

        return %data;
}

# Subroutine:	logwrite
# Args:		$type(scalar), $string (scalar)
# Return Value:	<void>
# Purpose:	Given a string, write it to the logfile. If the type is FATAL, kill the script and dump the last error.
sub logwrite($$)
{
        my $type = shift;
        my $msg = shift;

        my @time = localtime(time());
        my ($sec,$min,$hour,$mday,$mon,$year,$wday) = @time;
        my $tstamp = sprintf "%02d/%02d/%04d %02d.%02d.%02d", $mon+1, $mday, $year+1900, $hour, $min, $sec;

        print LOGFILE ("$tstamp: $hshLogTypes{$type}: $msg\n");

        if ($type == MXL_FATAL) 
	{ die "$msg\n$!\nCheck the log.\n"; }
}

# Subroutine:	process_900snapshot
# Args:
# Return Value:	($mta_inbound_load, $mta_outbound_load)
# Purpose:	Process, parse the 900SnapShot log and return the desired values.
sub process_900snapshot($$)
{
        my $subroutine_name = 'process_900snapshot';

	my ($hostname, $file) = @_;

	my $snapshot_file = '/tmp/900snapshot.log';

	if(($hostname eq '') || ($hostname eq 'localhost') || ($hostname eq '127.0.0.1')) {

		$snapshot_file = '/tmp/900snapshot.log';

	} else {
	
		my $rand = int(rand 1000000000);
		my $cmd = "scp -rp $hostname:$snapshot_file ";

		$snapshot_file = "/tmp/900snapshot.$rand.log";
		$cmd .= "$snapshot_file";

		if($debug_flag) { print "Command: $cmd\n"; }
		my $result =  `$cmd`;
	}

	my $date_str = sprintf("[%04d-%02d-%02d", @ymdhms);

	my $mta_inbound_load	= 'NaN';
	my $mta_outbound_load	= 'NaN';
	
	if(! -e $snapshot_file) {
	        my $warning = "Subroutine: $subroutine_name: 900SnapShot file was not found. No loading metrics will exist.";
	        if(($debug_flag) || ($verbose_flag))
	        { print "Warning: $warning\n"; }
	        logwrite(MXL_WARN, $warning);
		$warnings .= $warning;
	} else {
	        open(SNAPSHOT, "<$snapshot_file")
	                || logwrite(MXL_FATAL, "The 900SnapShot file did not open. Dying.");
	        my @data = <SNAPSHOT>;
	        close SNAPSHOT;
	
	        foreach my $line (@data) {
	                chomp($line);
	
	                my @parts = split(/ /, $line);
	
	                if(($parts[0] eq $date_str) && ( $parts[2] eq 'inbound'))
	                { $mta_inbound_load = $parts[3]; chop($mta_inbound_load); }
	                if(($parts[0] eq $date_str) && ( $parts[2] eq 'outbound'))
	                { $mta_outbound_load = $parts[3]; chop($mta_outbound_load); }
	        }
	}

	# Remove the tmp 900 Snapshot File.
	if((-e $snapshot_file) && ($snapshot_file ne '/tmp/900snapshot.log')) {
		my $cmd = "rm $snapshot_file";
		my $status = `$cmd` || logwrite(MXL_WARN, "The remote 900SnapShot file could not be removed. Dying.");
	}

	return ($mta_inbound_load, $mta_outbound_load);
}

# Subroutine:   rev_array
# Args:         $array (array)
# Return Value: $array (array)
# Purpose:      Given a array, return an array with elements in reverse order.
sub rev_array(@)
{
        my @one = @_;
        my $length = @one;
        my @two = ();
        for (my $ii=$length-1;$ii>=0;$ii--)
        { push @two, $one[$ii] }
        return @two;
}

# Subroutine:	querry_log_db_all_mesg_in
# Args:		none
# Return Value:	$string (scalar)
# Purpose:	Querry all log DBs and return total number of inbound messages. 
sub querry_log_db_all_mesg_in() 
{
	my $subroutine_name = 'querry_log_db_all_mesg_in';
	my $sum = 0;

	for my $db ( keys %dbs ) {
		
		if($dbs{$db} eq 'mxl_log') {

			my $dbh = DBI->connect("dbi:Pg:dbname=mxl_log;host=$db;port=$db_port", $db_user, $db_pass)
				 or logwrite(MXL_FATAL, "Subroutine:$subroutine_name: Could not connect to DB, $DBI::errstr");

			my $db_query =	sprintf(	"SELECT SUM(CASE WHEN field='Messages Total' ".
							"THEN value ELSE 0 END) FROM mxl_log_accum ".
                                			"WHERE TO_CHAR(created,'YYYY-MM-DD')='%04d-%02d-%02d' AND ".
                                			"domain!='system' AND domain!='bounce.mcafee.com';",
							$ymdhms[0], $ymdhms[1], $ymdhms[2]);


                	my $sth = $dbh->prepare("$db_query")
                        	or logwrite(MXL_FATAL, "Subroutine:$subroutine_name: Could not prepare DB querry, $db_query. $DBI::errstr");
                	$sth->execute()
                        	or logwrite(MXL_FATAL, "Subroutine:$subroutine_name: Could not execute DB querry, $db_query. $DBI::errstr");
                	my @row = $sth->fetchrow_array
                        	or logwrite(MXL_FATAL, "Subroutine:$subroutine_name: Could fetch the return value. $DBI::errstr");

			$sum = $sum + $row[0];

			if(($debug_flag) || ($verbose_flag)) {
				print "$subroutine_name: Query is:\n$db_query\n"; 
			}

			$sth->finish;
			$dbh->disconnect();
		}
	}

	return $sum;
}

# Subroutine:	querry_log_db_all_mesg_out
# Args:		none
# Return Value:	$string (scalar)
# Purpose:	Querry all log DBs and return total number of outbound messages. 
sub querry_log_db_all_mesg_out() 
{
	my $subroutine_name = 'querry_log_db_all_mesg_out';
	my $sum = 0;

	for my $db ( keys %dbs ) {
		
		if($dbs{$db} eq 'mxl_log') {

			my $dbh = DBI->connect("dbi:Pg:dbname=mxl_log;host=$db;port=$db_port", $db_user, $db_pass)
				 or logwrite(MXL_FATAL, "Subroutine:$subroutine_name: Could not connect to DB, $DBI::errstr");

			my $db_query =	sprintf(	"SELECT SUM(CASE WHEN field='Messages Total' ".
							"THEN value ELSE 0 END) FROM mxl_log_accum_out ".
                                			"WHERE TO_CHAR(created,'YYYY-MM-DD')='%04d-%02d-%02d' AND ".
                                			"domain!='system' AND domain!='bounce.mcafee.com';",
							$ymdhms[0], $ymdhms[1], $ymdhms[2]);


                	my $sth = $dbh->prepare("$db_query")
                        	or logwrite(MXL_FATAL, "Subroutine:$subroutine_name: Could not prepare DB querry, $db_query. $DBI::errstr");
                	$sth->execute()
                        	or logwrite(MXL_FATAL, "Subroutine:$subroutine_name: Could not execute DB querry, $db_query. $DBI::errstr");
                	my @row = $sth->fetchrow_array
                        	or logwrite(MXL_FATAL, "Subroutine:$subroutine_name: Could fetch the return value. $DBI::errstr");

			$sum = $sum + $row[0];

			if(($debug_flag) || ($verbose_flag)) {
				print "$subroutine_name: Query is:\n$db_query\n"; 
			}

			$sth->finish;
			$dbh->disconnect();
		}
	}

	return $sum;
}

# Subroutine:	querry_log_db_quar_mesg_in
# Args:		none
# Return Value:	$string (scalar)
# Purpose:	Querry all log DBs and return total number of quarantined inbound messages. 
sub querry_log_db_quar_mesg_in() 
{
	my $subroutine_name = 'querry_log_db_quar_mesg_in';
	my $sum = 0;

	for my $db ( keys %dbs ) {
		
		if($dbs{$db} eq 'mxl_log') {

			my $dbh = DBI->connect("dbi:Pg:dbname=mxl_log;host=$db;port=$db_port", $db_user, $db_pass)
				 or logwrite(MXL_FATAL, "Subroutine:$subroutine_name: Could not connect to DB, $DBI::errstr");

			my $db_query =	sprintf(	"SELECT SUM(CASE WHEN field='Messages Quarantined' ".
							"THEN value ELSE 0 END) FROM mxl_log_accum ".
                                			"WHERE TO_CHAR(created,'YYYY-MM-DD')='%04d-%02d-%02d' AND ".
                                			"domain!='system' AND domain!='bounce.mcafee.com';",
							$ymdhms[0], $ymdhms[1], $ymdhms[2]);


                	my $sth = $dbh->prepare("$db_query")
                        	or logwrite(MXL_FATAL, "Subroutine:$subroutine_name: Could not prepare DB querry, $db_query. $DBI::errstr");
                	$sth->execute()
                        	or logwrite(MXL_FATAL, "Subroutine:$subroutine_name: Could not execute DB querry, $db_query. $DBI::errstr");
                	my @row = $sth->fetchrow_array
                        	or logwrite(MXL_FATAL, "Subroutine:$subroutine_name: Could fetch the return value. $DBI::errstr");

			$sum = $sum + $row[0];

			if(($debug_flag) || ($verbose_flag)) {
				print "$subroutine_name: Query is:\n$db_query\n"; 
			}

			$sth->finish;
			$dbh->disconnect();
		}
	}

	return $sum;
}

# Subroutine:	sci_to_int
# Args:		$num (scalar)
# Return Value:	$string (scalar)
# Purpose:	Given a number in scientific notion, return the equivalent float. 
sub sci_to_int($)
{
	my($num) = @_;
	my $subroutine_name = 'sci_to_int';

	my @parts = split(/e/, $num);
	my $return_value = $parts[0];

	if(substr($parts[1],0,1) eq '-') {
		for(my $ii=0; $ii<$parts[1]; $ii++)
		{ $return_value = $return_value/10; }
	} else {
		for(my $ii=0; $ii<$parts[1]; $ii++)
		{ $return_value = $return_value*10; }
	}
	return $return_value;
}

# Subroutine:	summarized_data
# Args:		@data (array)
# Return Value:	$string (scalar)
# Purpose:	Given the data array, return a human-readable version of the daily metrics. 
sub summarize_data(@%%)
{
	my $subroutine_name = 'summarize_data';

	if($debug_flag)
	{ print "DEBUG: Entering $subroutine_name.\n"; }

	my ($ref1, $ref2, $ref3) = @_;
	my @data = @$ref1;
	my %arch_data = %$ref2;
	my %wds_data = %$ref3;

	my $expected_data_points = 17;

	if((my $length = @data) != $expected_data_points)
	{ logwrite(MXL_FATAL, "Subroutine: $subroutine_name: There are not enough data points to summarize ($length of $expected_data_points)."); } 

	if ($debug_flag > 1) {
		print "\tSubroutine: summarized_data: Displaying individual data points.\n";
		foreach my $datum (@data)
		{ print "\t\tDatum: $datum\n"; }
	}

	my $string =	
			"Level 3 In-Bound MTA Load:         $data[0]%\n".
			"Level 3 Out-Bound MTA Load:        $data[1]%\n".
			"Level 3 In-Bound MTAs:             $data[2]\n".
			"Data 393 In-Bound MTA Load:        $data[3]%\n".
			"Data 393 Out-Bound MTA Load:       $data[4]%\n".
			"Data 393 In-Bound MTAs:            $data[5]\n".
			"Total In-Bound MTA Load:           $data[6]%\n".
			"Total Out-Bound MTA Load:          $data[7]%\n".
			"Total Inbound Messages:            $data[8]\n".
			"Total Quarantine Messages:         $data[9]\n".
			"Total Outbound Traffic:            $data[10]\n".
#			"Quarantine DB Size                 $data[11]\n".
			"SQR Reports Generated              $data[12]\n".
			"SQR Reports Finished               $data[13]\n".
#			"DB Maintenance Finished            $data[14]\n". 
			"WDS CPL Length (lines)             $data[15]\n". 
			"WDS CPL Size (bytes)               $data[16]\n". 
			"";

	$string .=	"\n";

	foreach my $db (sort(keys %dbs)) {
		$string .=	summarize_db_stats($db);
	}

	$string .=	
			"\n---- Archiving Data ---- \n\n".
			"Number of Customers:               $arch_data{'num_cust'}\n".
			"Number of Seats:                   $arch_data{'num_seats'}\n".
			"Number of Messages Ingested:       $arch_data{'msgs_ingested'}\n".
			"Number of Messages Backlogged:     $arch_data{'msgs_backlogged'}\n".
			"Data 393 Store Size (b):           $arch_data{'data393_store_size'}\n".
			"Level 3 Store Size (b):            $arch_data{'level3_store_size'}\n".
			"Data 393 Index Size (b):           $arch_data{'data393_index_size'}\n".
			"Level 3 Index Size (b):            $arch_data{'level3_index_size'}\n".
			"Data 393 Searches:                 $arch_data{'data393_searches'}\n".
			"Level 3 Searches:                  $arch_data{'level3_searches'}\n".
			"";
	
	$string .=	
			"\n---- WDS Data ---- \n\n".
			"Minumum Latency:                   $wds_data{'min_latency'}\n".
			"Maximum Latency:                   $wds_data{'max_latency'}\n".
			"Average Latency:                   $wds_data{'avg_latency'}\n".
			"Average Peak Latency:              $wds_data{'avg_peak_latency'}\n".
			"Requests Allowed:                  $wds_data{'reqs_allowed'}\n".
			"Requests Blocked:                  $wds_data{'reqs_blocked'}\n".
			"Viruses Blocked:                   $wds_data{'viruses'}\n".
			"Bandwidth In (kb):                 $wds_data{'bw_in'}\n".
			"Bandwidth Out (kb):                $wds_data{'bw_out'}\n".
			"";

	return $string;
}

# Subroutine:	summarized_db_stats
# Args:		$db_ip (scalar)
# Return Value:	$string (scalar)
# Purpose:	Given the database ip, return a human-readable version of the daily metrics. 
sub summarize_db_stats($)
{
	my $subroutine_name = 'summarize_db_stats';

	if($debug_flag)
	{ print "DEBUG: Entering $subroutine_name.\n"; }

	my ($db) = @_;

	my @data = ();
	my $date_str = sprintf('%02d/%02d/%04d',$ymdhms[1],$ymdhms[2], $ymdhms[0]);
	my $expected_data_points = 12;
	my $found = 0;

	open(DB_STATS, "<$db_stats_log");
	my @lines = <DB_STATS>;
	close DB_STATS;

	foreach my $line (@lines) {
		if(($line =~ $db) && ($line =~ $date_str)) {
			if($debug_flag) {
				print "Subroutine: $subroutine_name - Found line in $db_stats_log for $db on $date_str.\n";
				print "Subroutine: $subroutine_name - $line\n";
			}	
			$found = 1;
			@data = split(/,/,$line,$expected_data_points);
		}
	}

	#if((my $length = @data) != $expected_data_points)
	#{ logwrite(MXL_FATAL, "Subroutine: $subroutine_name: There are not enough data points to summarize."); } 

	if ($debug_flag > 1) {
		print "\tSubroutine: summarized_data: Displaying individual data points.\n";
		foreach my $datum (@data)
		{ print "\t\tDatum: $datum\n"; }
	}

	my $string = '';
	unless($found) {
		$string = "No summary data for $dbs{$db} on $db\n";
		return $string;
	}

	unless(($data[0] eq $date_str) && ($data[2] eq $db) && ($data[1] eq $dbs{$db})) {
		logwrite(MXL_FATAL, "Subroutine: $subroutine_name: Data from file $db_stats_log does not match what is expected.\n");
	}

	$string =	
			"Database IP:                       $data[2]\n".
			"Database Name:                     $data[1]\n".
			"Disk Utilization:                  $data[3]\n".
			"Maintenance Start:                 $data[4]\n".
			"Maintenance End:                   $data[5]\n".
			"Maintenance Time:                  $data[6]\n".
			"Connections:                       $data[7]\n".
			"Load Average:                      $data[8]\n".
			"Raid Utilization:                  $data[9]\n".
			"Free Memory:                       $data[10]\n".
			"24-hour Load Average:              $data[11]\n";
	return $string;
}

# Subroutine:	usage
# Args:		<void>
# Return Value:	<void>
# Purpose:	Write the appropriate usage of metrics.pl to STDOUT. 
sub usage()
{
	if($debug_flag)
	{ print "DEBUG: Entering usage.\n"; }

	open(README, "<README.txt");
	my @lines = <README>;
	close README;

	foreach my $line (@lines)
	{ print $line; }
}

# Subroutine:	validate_override_time
# Args:		$override_time (scalar)
# Return Value:	@ymdhms (array)
# Purpose:	Given an override time string, validate it and return a representative @ymdhms array.
sub validate_override_time($)
{
	# This subroutine still needs error checking.
	my $subroutine_name = 'validate_override_time';

        if($debug_flag)
        { print "DEBUG: Entering $subroutine_name.\n"; }

	my ($time_string) = @_;

	my $length = length($time_string);

        if($debug_flag)
        { print "DEBUG: time_string = $time_string \n"; }

        if($debug_flag)
        { print "DEBUG: length = $length \n"; }

	my @date = (0,0,0,9,0,0);

	if($length >= 8) {
		$date[0] = substr($time_string,0,4);		
		$date[1] = substr($time_string,4,2);		
		$date[2] = substr($time_string,6,2);		
	}
	if($length >= 10) 
	{ $date[3] = substr($time_string,8,2); }
	if($length >= 12) 
	{ $date[4] = substr($time_string,10,2); }
	if($length >= 14) 

	{ $date[5] = substr($time_string,14,2); }

        if($debug_flag)
        { print "\tSubroutine: $subroutine_name: New date is @date\n"; }

	return @date;
}

# Subroutine:	write_daily_data
# Args:		@data (array)
# Return Value:	<void>
# Purpose:	Given the data array, write the appropriate entry in the daily data file. 
sub write_daily_data(@%%)
{
	my $subroutine_name = 'write_daily_data';

	if($debug_flag)
	{ print "DEBUG: Entering $subroutine_name.\n"; }

	my ($ref1, $ref2, $ref3) = @_;
	my @data = @$ref1;
	my %arch_data = %$ref2;
	my %wds_data = %$ref3;

	my $daily_output_directory      = "$data_dir/daily";
	my $daily_output_file_format    = '%04d-%02d-%02d.txt';

	my $outfile = $daily_output_directory."/".sprintf($daily_output_file_format,@ymdhms); 
	if($debug_flag)
	{ print "\tSubroutine: $subroutine_name: writing to $outfile\n"; }

	open (OUTFILE,">$outfile")
		|| logwrite(MXL_FATAL, "Subroutine: $subroutine_name: Failed to open outfile, $outfile.");
	print OUTFILE "# Metrics for Denver Pod ".$pod." on ".sprintf("%04d-%02d-%0-2d", @ymdhms )."\n";
	print OUTFILE "# Generated: ".sprintf("%04d-%02d-%02d %02d.%02d.%02d", @run_time)."\n\n";
        print OUTFILE &summarize_data(\@data, \%arch_data, \%wds_data); 

	logwrite(MXL_INFO, "Daily summary written to $outfile.");
	close OUTFILE;
}

# Subroutine:	write_monthly_data
# Args:		@data (array)
# Return Value:	<void>
# Purpose:	Given the data array, write the appropriate entry in the monthly data file. 
sub write_monthly_data(@%%)
{
	my ($ref1, $ref2, $ref3) = @_;
	my $subroutine_name = 'write_monthly_data';

	if($debug_flag)
	{ print "DEBUG: Entering $subroutine_name.\n"; }

	my @data	= @$ref1;
	my %arch_data	= %$ref2;
	my %wds_data	= %$ref3;

	my $monthly_output_directory	= "$data_dir/monthly";
	my $monthly_output_file_format	= '%04d-%02d.csv';

	my $date 	= sprintf("%04d-%02d-%02d", @ymdhms);
	my $found_flag	= 0;
	my $header	=	"Date(Y-M-D),".
				"L3 IBLoad(\%),L3 OBLoad(\%),".
				"L3 IB MTAs(#),".
				"Data 393 IBLoad(\%),Data 393 OBLoad(\%),".
				"D393 IB MTAs(#),".
				"Total IBLoad(\%),Total OBLoad(\%),".
				"IBMesg(#),QMesg(#),OBMesg(#),QuarSize(#),".
				"SQRReports(#),SQRFinshed(h:m:s),".
				"DBMaintFinished(h:m:s),".
				"CPL Length (lines),CPL Size (bytes),".
				"name,IP,disk utilization,maintenance start,maintenance end,maintenance time,connections,".
				"load average,\%raid,free memory,24-hour load average,".
				"name,IP,disk utilization,maintenance start,maintenance end,maintenance time,connections,".
				"load average,\%raid,free memory,24-hour load average,".
				"name,IP,disk utilization,maintenance start,maintenance end,maintenance time,connections,".
				"load average,\%raid,free memory,24-hour load average,".
				"name,IP,disk utilization,maintenance start,maintenance end,maintenance time,connections,".
				"load average,\%raid,free memory,24-hour load average,".
				"name,IP,disk utilization,maintenance start,maintenance end,maintenance time,connections,".
				"load average,\%raid,free memory,24-hour load average,".
				"arch customers, arch seats, arch messages, arch backlog,".
				"arch data 393 store, arch level 3 store,".
				"arch d393 index size, arch l3 index size,".
				"arch d393 searches, arch l3 searches,".
				"wds min latency, wds max latency, wds avg latency, wds avg peak latency,".
				"wds requests allowed, wds requests blocked, wds viruses blocked,".
				"wds bandwidth in, wds bandwidth out".
				"\n";
	my $line_format	=	'%s,%3.2f,%3.2f,%d,%3.2f,%3.2f,%d,%3.2f,%3.2f,%d,%d,%d,%d,%d,%s,%s,%d,%d';
	my $outfile	=	$monthly_output_directory."/".sprintf($monthly_output_file_format, @ymdhms); 

	my $new_line	=	sprintf($line_format, $date, @data);

	foreach my $db (sort(keys %dbs)) {
		$new_line = $new_line.",".&generate_db_stats_cvs_entry($db);
	}

	my @new_data;

	push @new_data, $arch_data{'num_cust'};
	push @new_data, $arch_data{'num_seats'};
	push @new_data, $arch_data{'msgs_ingested'};
	push @new_data, $arch_data{'msgs_backlogged'};
	push @new_data, $arch_data{'data393_store_size'};
	push @new_data, $arch_data{'level3_store_size'};
	push @new_data, $arch_data{'data393_index_size'};
	push @new_data, $arch_data{'level3_index_size'};
	push @new_data, $arch_data{'data393_searches'};
	push @new_data, $arch_data{'level3_searches'};

	push @new_data, $wds_data{'min_latency'};
	push @new_data, $wds_data{'max_latency'};
	push @new_data, $wds_data{'avg_latency'};
	push @new_data, $wds_data{'avg_peak_latency'};
	push @new_data, $wds_data{'reqs_allowed'};
	push @new_data, $wds_data{'reqs_blocked'};
	push @new_data, $wds_data{'viruses'};
	push @new_data, $wds_data{'bw_in'};
	push @new_data, $wds_data{'bw_out'};

	$line_format	= '%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%f,%f,%f,%f,%d,%d,%d,%f,%f';
	$new_line	= $new_line.",".sprintf($line_format, @new_data);
				
	if(! -e $outfile)
	{ system("touch $outfile"); }	# On the first day of the month, the file needs to be created.

	open(OUTFILE, "<$outfile")
		|| logwrite(MXL_FATAL, "Subroutine: $subroutine_name could not open the monthly data file, $outfile, for reading.");
	my @lines = <OUTFILE>;
	close OUTFILE;

	open(OUTFILE, ">$outfile")
		|| logwrite(MXL_FATAL, "Subroutine: $subroutine_name could not open the monthly data file, $outfile, for writing.");
	
	if($lines[0] ne $header)
	{ print OUTFILE $header; }

        foreach my $line (@lines) {
		my @parts = split(/,/,$line);
		if ($debug_flag > 1) {
			print "\tSubroutine: $subroutine_name: Parsing a line of the monthly data file.\n";
			print "\tThe original line was $line.\n";
			foreach my $part (@parts)
			{ print "\t\tPart: $part\n"; }
		}
		if ($parts[0] eq $date) {
			$found_flag = 1;
			print OUTFILE $new_line,"\n"; 
		} else {
			print OUTFILE $line;
		}
	}

	if(! $found_flag) {
		print OUTFILE $new_line,"\n";
	}

	logwrite(MXL_INFO, "Entry made to monthly data file $outfile.");
	close OUTFILE;
}

# Subroutine:	ymdhms_human2unix
# Args:		@time (array)
# Return Value:	@time (array)
# Purpose:	Given the human-readable date/time array, return the representative unix array. 
sub ymdhms_human2unix(@)
{
	my $subroutine_name = 'ymdhms_human2unix';
	my @array = @_;
	my $length = @array;
	unless($length == 6) {
		logwrite(MXL_INFO, "$subroutine_name expects an array of length 6");
	}
	return ($array[0]-1900,$array[1]-1,$array[2],$array[3],$array[4],$array[5]);
}

# Subroutine:	ymdhms_unix2human
# Args:		@time (array)
# Return Value:	@time (array)
# Purpose:	Given the date/time array, return the representative human-readable array. 
sub ymdhms_unix2human(@)
{
	my $subroutine_name = 'ymdhms_unix2human';
	my @array = @_;
	my $length = @array;
	unless($length == 6) {
		 logwrite(MXL_INFO, "$subroutine_name expects an array of length 6"); 
	}
	return ($array[0]+1900,$array[1]+1,$array[2],$array[3],$array[4],$array[5]);
}
###EOF
################################################################################################
