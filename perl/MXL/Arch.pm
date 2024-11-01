# Perl module with MX Logic archiving-specific functionality.
package Arch;

use DBI;
use XML::Simple;

our $AUTHOR	= 'jeckhardt@mxlogic.com';
our $VERSION	= 0.1;
our $PODCONFIG	= '/mxl/etc/pod_config.xml';

# Return an array of the CIDs seen in the arc_mail_source table.
# I want to depricate this function in favor of select_arc_mail_source().
sub cids_from_mail_source {

	my $xs = XML::Simple->new();
	my $config = $xs->XMLin($PODCONFIG)
	        || die "Can't read $PODCONFIG xml file. $!\n";
	
	my $db = $config->{'pod'}->{'db'}->{'id'};
	my $db_ip = $config->{'pod'}->{'db'}->{'path'};
	my $db_user = $config->{'pod'}->{'db'}->{'username'};
	my $db_pass = $config->{'pod'}->{'db'}->{'password'};
	my $db_port = $config->{'pod'}->{'db'}->{'port'};
	
	my $db_source   = "dbi:Pg:dbname=$db;host=$db_ip;port=$db_port";
	my $db_querry   = "SELECT DISTINCT id FROM arc_mail_source WHERE active = 1";
	
	my $dbh = DBI->connect("$db_source", "postgres", $db_pass, { PrintError => 1, RaiseError=> 0, AutoCommit => 0 })
	        or warn "Could not connect to DB, $DBI::errstr" && return 0;
	my $sth = $dbh->prepare("$db_querry")
	        or warn "Could not prepare DB querry, $db_querry. $DBI::errstr" && return 0;
	$sth->execute()
	       or warn "Could not execute DB querry, $db_querry. $DBI::errstr" && return 0;
	
	my @cids = ();
	
	while(my @row = $sth->fetchrow_array) {
	        push @cids, $row[0];
	}
	
	$dbh->disconnect;
	return @cids;
}

# Given a CID, return an array with directories containing that customer's data.
sub cust_dirs($) {

	my ($cid) = @_;

	my $root_dir = '/mxl/msg_archive/mas/.store/entity/';
	#my @store_dirs = `ls -1 $root_dir`;
	###my @store_dirs = ('data393', 'level3');
	my @store_dirs = ('latisys2', 'hosting2');
	my @cust_dirs = ();
	
        my $hex = sprintf("%x",$cid);
        my $length = length($hex) + length($hex)%2;
        my $str = '%0'.$length.'x';
        my $hexi = sprintf($str,$cid);
        my $path = '/';
        for(my $ii=0; $ii<$length; $ii=$ii+2) {
                $path .= sprintf("%s/",substr($hexi,$ii,2));
        }
        #print "$cid, $str, $hexi, $path\n";

        foreach my $dir (@store_dirs) {
                chomp($dir);
                my $cust_path = $root_dir.$dir.$path.'cust';
                #print "$cust_path\n";
		push @cust_dirs, $cust_path;
        }

	return @cust_dirs;
}

# Given a SID, return array containing the mailsource details.
sub ms_from_sid($) {

	my ($sid) = @_;

	my $xs = XML::Simple->new();
	my $config = $xs->XMLin($PODCONFIG)
	        || die "Can't read $PODCONFIG xml file. $!\n";
	
	my $db = $config->{'pod'}->{'db'}->{'id'};
	my $db_ip = $config->{'pod'}->{'db'}->{'path'};
	my $db_user = $config->{'pod'}->{'db'}->{'username'};
	my $db_pass = $config->{'pod'}->{'db'}->{'password'};
	my $db_port = $config->{'pod'}->{'db'}->{'port'};
	
	my $db_source   = "dbi:Pg:dbname=$db;host=$db_ip;port=$db_port";
	my $db_querry   = "SELECT * FROM arc_mail_source WHERE server_id = $sid LIMIT 1;";
	
	my $dbh = DBI->connect("$db_source", "postgres", $db_pass, { PrintError => 1, RaiseError=> 0, AutoCommit => 0 })
	        or warn "Could not connect to DB, $DBI::errstr" && return 0;
	my $sth = $dbh->prepare("$db_querry")
	        or warn "Could not prepare DB querry, $db_querry. $DBI::errstr" && return 0;
	$sth->execute()
		or warn "Could not execute DB querry, $db_querry. $DBI::errstr" && return 0;

	#print "$db_querry\n";

        my $hashref;
        my %hash;

        while ( my $ref = $sth->fetchrow_hashref() ) {
                $hashref = $ref;
                #%hash = %$ref;
        }

        $dbh->disconnect;

        return $hashref;
        #return %hash;
}

# Return an array of the SIDs seen in the arc_mail_source table.
# This function returns all server_id values in the arc_mail_source table.
# Optionally, you can submit a hash that contains key value pairs to narrow the results.
sub select_arc_mail_source(%) {

	my (%args) = @_;

	my $xs = XML::Simple->new();
	my $config = $xs->XMLin($PODCONFIG)
	        || die "Can't read $PODCONFIG xml file. $!\n";
	
	my $db = $config->{'pod'}->{'db'}->{'id'};
	my $db_ip = $config->{'pod'}->{'db'}->{'path'};
	my $db_user = $config->{'pod'}->{'db'}->{'username'};
	my $db_pass = $config->{'pod'}->{'db'}->{'password'};
	my $db_port = $config->{'pod'}->{'db'}->{'port'};
	
	my $db_source   = "dbi:Pg:dbname=$db;host=$db_ip;port=$db_port";
	my $db_querry   = "SELECT DISTINCT server_id FROM arc_mail_source WHERE active = 1 ";

	foreach my $key (keys %args) {
		$db_querry .= "AND $key = '$args{$key}' ";
	}
	
	#print "$db_querry\n";

	my $dbh = DBI->connect("$db_source", "postgres", $db_pass, { PrintError => 1, RaiseError=> 0, AutoCommit => 0 })
	        or warn "Could not connect to DB, $DBI::errstr" && return 0;
	my $sth = $dbh->prepare("$db_querry")
	        or warn "Could not prepare DB querry, $db_querry. $DBI::errstr" && return 0;
	$sth->execute()
	       or warn "Could not execute DB querry, $db_querry. $DBI::errstr" && return 0;
	
	my @sids = ();
	
	while(my @row = $sth->fetchrow_array) {
	        push @sids, $row[0];
	}
	
	$dbh->disconnect;
	return @sids;
}


# Given a CID return an array with all SIDs that are active.
sub sids_from_cid($) {

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
	my $db_querry   = "SELECT server_id FROM arc_mail_source WHERE active = 1 AND id = $cid;";

	###print "CID from sids_from_cid = $cid \n";
	
	my $dbh = DBI->connect("$db_source", "postgres", $db_pass, { PrintError => 1, RaiseError=> 0, AutoCommit => 0 })
	        or warn "Could not connect to DB, $DBI::errstr" && return 0;
	my $sth = $dbh->prepare("$db_querry")
	        or warn "Could not prepare DB querry, $db_querry. $DBI::errstr" && return 0;
	$sth->execute()
	       or warn "Could not execute DB querry, $db_querry. $DBI::errstr" && return 0;
	
	my @sids = ();

        while(my @row = $sth->fetchrow_array) {
                push @sids, $row[0];
        }

	$dbh->disconnect;

	return @sids;
}

1;
