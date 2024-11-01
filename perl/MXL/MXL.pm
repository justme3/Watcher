# Perl module with MX Logic archiving-specific functionality.
package MXL;

use DBI;
use XML::Simple;

our $AUTHOR	= 'jvossler@mxlogic.com';
our $VERSION	= 0.1;
our $PODCONFIG	= '/mxl/etc/pod_config.xml';

# Return an array of the CIDs seen in the arc_mail_source table.
sub customer_from_cid {

	my ($cid) = @_;

	my $xs = XML::Simple->new();
	my $config = $xs->XMLin($PODCONFIG)
	        || die "Can't read $PODCONFIG xml file. $!\n";
	
	my $db = $config->{'pod'}->{'db'}->{'id'};
	my $db_ip = $config->{'pod'}->{'db'}->{'path'};
	my $db_user = $config->{'pod'}->{'db'}->{'username'};
	my $db_pass = $config->{'pod'}->{'db'}->{'password'};
	my $db_port = $config->{'pod'}->{'db'}->{'port'};
	
	my $db_source   = "dbi:Pg:dbname=$db;host=$db_ip;port=$db_port";
	my $db_querry   = "SELECT * FROM mxl_customer WHERE customer_id = $cid LIMIT 1;";
	
	my $dbh = DBI->connect("$db_source", "postgres", $db_pass, { PrintError => 1, RaiseError=> 0, AutoCommit => 0 })
	        or warn "Could not connect to DB, $DBI::errstr" && return 0;
	my $sth = $dbh->prepare("$db_querry")
	        or warn "Could not prepare DB querry, $db_querry. $DBI::errstr" && return 0;
	$sth->execute()
	       or warn "Could not execute DB querry, $db_querry. $DBI::errstr" && return 0;

	my $hashref;

	while ( $ref = $sth->fetchrow_hashref() ) {
		$hashref = $ref;
	}

	$dbh->disconnect;

	return $hashref;
}

sub customer_seats_from_cid {

	my ($cid) = @_;

	my $xs = XML::Simple->new();
	my $config = $xs->XMLin($PODCONFIG)
	        || die "Can't read $PODCONFIG xml file. $!\n";
	
	my $db = $config->{'pod'}->{'db'}->{'id'};
	my $db_ip = $config->{'pod'}->{'db'}->{'path'};
	my $db_user = $config->{'pod'}->{'db'}->{'username'};
	my $db_pass = $config->{'pod'}->{'db'}->{'password'};
	my $db_port = $config->{'pod'}->{'db'}->{'port'};
	
	my $db_source   = "dbi:Pg:dbname=$db;host=$db_ip;port=$db_port";
	my $db_querry   = "select eds_billed_users_qty from mxl_customer where customer_id = $cid;";
	
	my $dbh = DBI->connect("$db_source", "postgres", $db_pass, { PrintError => 1, RaiseError=> 0, AutoCommit => 0 })
	        or warn "Could not connect to DB, $DBI::errstr" && return 0;
	my $sth = $dbh->prepare("$db_querry")
	        or warn "Could not prepare DB querry, $db_querry. $DBI::errstr" && return 0;
	$sth->execute()
	       or warn "Could not execute DB querry, $db_querry. $DBI::errstr" && return 0;

	my $seats = 0;

	while (my @row = $sth->fetchrow_array()) {
		$seats = $row[0];
	}

	$dbh->disconnect;

	return $seats;
}

1;
