#!/usr/bin/perl
# File: check_postgres.pl
# Desc: Performs master/slave replication checking
# Code: by Shinto on 2008-07-29, last updated 2010-10-06

# TODO: Sequence checks
#       - Get a list of sequences (-c "\d" |grep sequence |cut -d'|' -f2)
#       - Look up last_value
#       - Compare to value in /tmp/check_postgres.pl.stat
#       - Alert if difference is over some limit (10,000, for example)
#       - Save last_value to /tmp/check_postgres.pl.stat


use strict;

my $interval = '24 hours';	# Specify a threshold for long-running queries.
				#+ Use a format compatible with ::interval

my $host = `hostname`; chomp($host);
$host =~ s/p(\d+)c(\d+)\D(\d+).*/10.$1.1$2.$3/;
$host =~ s/\.0+/./g;
my $db   = $ARGV[0];
my $psql = '/usr/bin/psql';	# Path to 'psql' binary

# List servers that are supposed to be running as masters
#my @master_hosts   = ('10.1.106.130','10.2.106.130','10.3.106.130');
my @master_hosts   = ();
# List servers that are supposed to be running as slaves
#my @slave_hosts    = ('10.1.107.130','10.2.107.130','10.3.107.130');
my @slave_hosts    = ();

if (!$db || $db =~ /-h/) {
	print "Usage: $0 [Database name]\n";
	print "Example: check_postgres_replication.pl\n";
	print "This script uses the hostname to determine the proper IP\n";
	print "address on which postgres is running.\n";
	exit(1);
}


# The SQL I want to run, in this case, show tables.
my $sql='"\dt"';

# Fetch database parameters
my $pod		= `grep POD= /mxl/bin/mod_settings |sed 's/.*=/-P /'`; chomp($pod);
my $get_config	= "/mxl/bin/get_config_entry -f /mxl/etc/pod_config.xml $pod";
# Parameters for connecting to the database.  I'll just pull the policy DB's
#+ data and assume every DB uses the same username and password
my $dbuser	= `$get_config -S "pod/db?username"`    ; chomp($dbuser);
my $dbpass	= `$get_config -S "pod/db?password"`    ; chomp($dbpass);

if (!$dbpass) {
	print "Unable to extract DB info from pod_config.xml!\n";
	exit 2;		# I'd say that's a critical error
}
$ENV{'PGPASSWORD'} = $dbpass;

my $nagtext;	# Nagios text to display
my $return=0;	# Nagios return code, 0=OK, 1=WARN, 2=CRIT, 4=UNKNOWN


# Check for master/slave processes
my $is_slave=grep(/$host/,@slave_hosts);
my $is_master=grep(/$host/,@master_hosts);

# Perform a simple query, in this case, one that lists all the tables
#print "$psql -U $dbuser -h $host -At $db -c $sql 2>&1\n";
my $query = `$psql -U $dbuser -h $host -At $db -c $sql 2>&1`; chomp($query);

# I expect my "\dt" to output something like:qu
#+ public|mxl_customer|table|postgres
#+ Key off 'table|postgres' to see if I have valid output or not
my $query_ok=0;

$query_ok=1 if ($query =~ /table\|[postgres|threat]/);

# This is a known issue with Postgres 8.3, fixed in 8.4.
if ($query =~ /cache lookup failed/) {
	$nagtext = "UNKNOWN: $query";
	$return=3;	# Unknown
	print "$nagtext\n"; exit($return);
}

#my $slave_ok =`ps ax |grep 'sh -c' |grep ha_copy.sh |grep -v grep |wc -l`;
# Check for ha_copy.sh, get the process start time
my $slave_ok = `ps -o start=,args= -C sh |grep ha_copy.sh |grep -v grep`;
chomp($slave_ok);
# If ha_copy.sh isn't running, but should be, wait a few seconds and check again
if ($is_slave && !$slave_ok) {
	sleep 10;
	$slave_ok = `ps -o start=,args= -C sh |grep ha_copy.sh`;
	chomp($slave_ok);
}

my $master_ok=`ps ax |grep postgres: |grep archiver |grep -v grep |wc -l`;
chomp($master_ok);


# Look for long-running queries
my $long_running=0;	# Number of long-running queries detected
my $longtext; my $pid ; my $start;
unless ($is_slave) {
	my $sql="SELECT query_start,procpid FROM pg_stat_activity WHERE current_query!='<IDLE>' AND query_start < NOW() - '$interval'::interval ORDER BY query_start";
	my $longtext;		# Carries return text for Nagios
	my $query = `$psql -U $dbuser -h $host -At $db -c "$sql" 2>&1`; chomp($query);
	foreach my $line (split(/\n/,$query)) {
		$long_running++;
		($start,$pid) = split(/\|/,$line);
	}
}
# Print out some detail if there's one long-running query.  Just print a number
#+ if there is more than one long-running query.
if ($long_running==1 && $pid) {
	$longtext .= "<Br>WARN: Query PID $pid has been running since $start";
	$return=1 unless $return>1;
} elsif ($long_running>1) {
	$longtext .= "<Br>WARN: $long_running long-running queries detected";
	$return=1 unless $return>1;
}


# Fetch the current date
my ($D,$M,$Y) = (localtime(time))[3,4,5];   $M++;   $Y+=1900;
my $date=sprintf("%04d%02d%02d",$Y,$M,$D);
# Look for WAL errors on masters or slaves
my $errors;
if ($is_master || $is_slave) {
	my $crit_wal_errors=0;
	my $warn_wal_errors=0;
	my $archive_errors=0;
	foreach my $log (</var/log/mxl/postgres.log.$date*>) {
		open(LOG,'<'.$log);
		while (<LOG>) {
			if (/WAL ERROR/) {
				if (/Could not write out \/tmp\/.wal_log.processed.list.tmp/) {
					$warn_wal_errors++;
				} else {
					$crit_wal_errors++;
				}
			}
			$archive_errors++ if (/Archive/);
		}
		close(LOG);
	}
	if ($warn_wal_errors) {
		$errors="<Br>WARNING: Found $warn_wal_errors non-critical WAL errors in postgres.log.$date*";
		$return=1;
	}
	if ($crit_wal_errors) {
		$errors="<Br>CRITICAL: Found $crit_wal_errors WAL errors in postgres.log.$date*";
		$return=2;
	}
	if ($is_master && $archive_errors) {
		$errors.="<Br>CRITICAL: Found $archive_errors archive errors in postgres.log.$date*";
		$return=2;
	}
}

# Check the general status of the DB, plus sub-processes for slaves & masters
if ($is_slave) {
	# Slaves should return "FATAL:  the database system is starting up"
	if ($query =~ /database system is starting up/) {
		$nagtext = 'postgres is in slave mode';
		if ($slave_ok) {
			$nagtext .= ', ha_copy.sh is running';
			$nagtext = 'OK: ' . $nagtext;
		} else {
			$nagtext .= ' but ha_copy.sh isn\'t running';
			$nagtext = 'CRITICAL: ' . $nagtext;
			$return=2;
		}
	} elsif ($query_ok) {
		$nagtext = 'postgres isn\'t in slave mode';
		$nagtext = 'CRITICAL: ' . $nagtext;
		$return=2;
		if ($slave_ok) {
			$nagtext .= ' (ha_copy.sh _is_ running)';
		} else {
			$nagtext .= ' and ha_copy.sh isn\'t running.  Is this DB now the master?';
		}
	} elsif ($slave_ok =~ /\D/) {
		$nagtext = "CRITICAL: ha_copy.sh appears to be stuck, the last iteration started prior to today.";
		$return=2;
	} else {
		$nagtext = "CRITICAL: $query";
		$return=2;
		print "$nagtext\n"; exit($return);
	}
} elsif ($is_master) {
	if (!$query_ok) {
		$nagtext = "CRITICAL: $query";
		$return=2;
		print "$nagtext\n"; exit($return);
	}
	if ($master_ok) {
		if ($query_ok) {
			$nagtext = "$db OK, and archiver process is running";
		}
	} else {
		$nagtext = 'CRITICAL: Archiver process is not running! Check WAL archive status!';
		$return=2;
	}
} else {
	if ($query_ok) {
		if ($master_ok) {
			$nagtext = "WARN: $db is OK, but archiver is running (it shouldn't be on this box)";
			$return=1 unless ($return>1);
		} elsif ($slave_ok) {
			$nagtext = "WARN: $db is OK, but ha_copy.sh is running (it shouldn't be on this box)";
			$return=1 unless ($return>1);
		} else {
			$nagtext = "$db OK";
		}
	} else {
		$nagtext = 'CRITICAL: ' . $query;
		$return=2;
	}
}



print $nagtext . $errors . $longtext . "\n"; exit($return);

