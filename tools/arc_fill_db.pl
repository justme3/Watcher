#!/usr/bin/perl

# Script: arc_dump_ms.pl
# By: jvossler
# Dated: 28 Jan 2009
# Purpose: tool to populate an sqlite3 db at /tmp/arc.cust.data.db

# The databases has been created as follows.
# sqlite3 /tmp/arc.cust.data.db "create table seq (key INTEGER PRIMARY KEY,snum INTEGER, cid INTEGER, timestamp DATE);"
# sqlite3 /tmp/arc.cust.data.db "create table backlog (server_id INTEGER PRIMARY KEY, cid INTEGER, timestamp DATE,
#				 total INTEGER, five INTEGER, ten INTEGER, fifteen INTEGER, twenty INTEGER, twentyfive INTEGER);"


our $author		= 'jvossler';
our $author_email	= 'John_Vossler\@McAfee.com';
our $last_update	= '01-28-2009';
our $script_name	= 'arc_fill_db.pl';
our $script_version	= '0.1';

use strict;
use warnings;
#use diagnostics;

use lib '/usr/local/bin/opsadmin/perl/';

use DBI;
use Data::Dumper;
use Getopt::Long qw(:config no_ignore_case bundling);
use MXL::Arch;
use MXL::MXL;
use XML::Simple;

# Globals.
our %flags;

# Subroutine prototypes
sub banner();
sub options();
sub usage();

&main();

### Begin MAIN ###
sub main() {

	%flags = &options();
	
	my %args = ();

	my @cids = Arch::cids_from_mail_source();

	foreach my $cid (@cids) {

		my $snum = `/mxl/sbin/mas seq cust=$cid`;
		chomp($snum);
		if($snum eq '') { next; }

		my $insert = "sqlite3 /tmp/arc.cust.data.db \"insert into seq (cid,snum,timestamp) values ($cid,$snum,DATETIME('NOW'));\"";
		system("$insert");

		if(($flags{'debug'}) || ($flags{'verbose'})) {
			print "Customer: $cid\n";
			print "Sequence Number: $snum\n";
			print "Insert: $insert\n";
		}
		
	}
}
### End MAIN ###

### Subroutines ###
#
# Subroutine:	banner
# Args:		<void>
# Return Value:	<void>
# Purpose:	Print the script's basic info to STDOUT. 
sub banner()
{
	print "$script_name by: $author\n";
	print "Version: $script_version\n";
	print "Last Update: $last_update\n";
}
#
# Subroutine:   get_filesystems.
# Args:         none
# Return Value: indexing filesytems
# Purpose:      Parse /mxl/etc/mxl_message_archiving.xml and get the designated filesystems 
sub get_filesystems()
{

        my $config = '/mxl/etc/mod_message_archive.xml';
	my $ref = XMLin("$config");

        #my $xp = XML::XPath->new(filename => $config)
        #        || die "Can't read $config as XML: $!\n";

  	print Dumper($ref);

	my $queue	=  $ref->{'config'}->{'IndexQueue'}->{'local'};
	my $rqueue	=  $ref->{'config'}->{'IndexQueue'}->{'remote'};
	my $saved	=  $ref->{'config'}->{'IndexQueue'}->{'local_save_dir'};
	my $reject	=  $ref->{'config'}->{'IndexQueue'}->{'local_reject_dir'};
	
	#print "$queue $rqueue $saved $reject\n";
	return ($queue,$rqueue,$saved,$reject);
}
#
# Subroutine:	options
# Args:		<void>
# Return Value:	<void>
# Purpose:	 Process Command Line Options.
sub options()
{
	#Getopt::Long::Configure ("bundling");

	# Global Flags
	my %hash = (
			'debug'		=> 0,
			'help'		=> 0,
			'verbose'	=> 0,
			'version'	=> 0,
		);

	my $result = GetOptions (	
					"d+"		=> \$hash{'debug'},	# incremental
					"debug+"	=> \$hash{'debug'},	# incremental
					"h"		=> \$hash{'help'},	# binary
					"help"		=> \$hash{'help'},	# binary
					"host=s"	=> \$hash{'host'},	# string
					'n=i',		=> \$hash{'num'},	# integer
					"port=s"	=> \$hash{'port'},	# string
					"user=s"	=> \$hash{'user'},	# string
					"v"		=> \$hash{'verbose'},	# binary
					"verbose"	=> \$hash{'verbose'},	# binary
					"V"		=> \$hash{'version'},	# binary
				);

	if($flags{'version'}) { &banner; die; }

	unless($result) {
		&usage();
		die "There was an issue interperting the options you've set. Please check your usage.\n";
	}

	unless(defined($hash{'num'})) { $hash{'num'} = 5; }

	if($hash{'debug'}) {
		print "Options Selected:\n";
		print Dumper(\%hash); 
	}

	return %hash;
}
#
# Subroutine:	usage
# Args:		<void>
# Return Value:	<void>
# Purpose:	Write the appropriate usage to STDOUT. 
sub usage()
{

# Please forgive this formating
# ... usage() is my exception to otherwise clean code these days.
my $usage = <<EOF;
Usage: $0 -p 1 [OPTIONS]

	To be written ...
EOF

	print $usage;
}
#
# Subroutine:	summarize_imap
# Args:		%server hash
# Return Value:	Prints summary for and returns 0 for success, 1 for error.
# Purpose:	Print the script's basic info to STDOUT. 
sub summarize_imap()
{
	my (%server) = @_;

	my %samples;
	my %stats;

	my $server_str = "Checking mailbox: $server{user}\@$server{'host'}:$server{'port'}";

	#if(($flags{'debug'}) || ($flags{'verbose'})) {
	print "Checking mailbox: $server{user}\@$server{'host'}:$server{'port'}\n";
	#}

	my $imap;
	my $socket;

	if($server{'port'} == 993) {
		#print "SSL\n";
		$socket = IO::Socket::SSL->new(
				Proto    => 'tcp',
				PeerAddr => $server{'host'},
				PeerPort => $server{'port'},
			) || die "Could not create socket on $server_str\n";


		$imap = Mail::IMAPClient->new(
				Password	=> $server{'pass'},
				Peek		=> 1,
				Socket  	=> $socket,
				User		=> $server{'user'},
			) or die "Could connect to \n$!\n";
	} else {
		#print "No SSL\n";
		$imap = Mail::IMAPClient->new(
				Server		=> $server{'host'},
				Password	=> $server{'pass'},
				Peek		=> 1,
				User		=> $server{'user'},
			) or die "Could connect to \n$!\n";
	}

	#my @folders = $imap->folders();
	#foreach my $folder (@folders) {
	#	if($folder eq 'INBOX') {
	#		print "Folder: $folder exists!\n";
	#	}
	#}

	$stats{'count'}	= 0;
	$stats{'1M'}	= 0;
	$stats{'5M'}	= 0;
	$stats{'10M'}	= 0;
	$stats{'15M'}	= 0;
	$stats{'20M'}	= 0;
	$stats{'25M'}	= 0;
	$stats{'total'}	= 0;

	$imap->select('INBOX') || die "Could not select INBOX.\n";
	#$imap->select('Journal') || die "Could not select Journal.\n";

	my @msgs = $imap->messages() or warn "Could not list messages: $@\n";

        foreach my $message (@msgs) {

		my $size = $imap->size($message);

		if($stats{'count'} < $flags{'num'}) {
			;
		}
		$stats{'total'} += $size;

		if($size <=	 1000000) { $stats{'1M'}++; }
		elsif($size <=	 5000000) { $stats{'5M'}++; }
		elsif($size <=	10000000) { $stats{'10M'}++; }
		elsif($size <=	15000000) { $stats{'15M'}++; }
		elsif($size <=	20000000) { $stats{'20M'}++; }
		elsif($size <=	25000000) { $stats{'25M'}++; }
		else { ; }

		$stats{'count'}++;
        }

	if($stats{'count'} == 0) {
		$stats{'avg'} = 0;
	} else {
		$stats{'avg'} = $stats{'total'}/$stats{'count'};
	}

	return (%stats, %samples);
}
#
# Subroutine:	summarize_pop3
# Args:		%server hash
# Return Value:	Prints summary for and returns 0 for success, 1 for error.
# Purpose:	Print the script's basic info to STDOUT. 
sub summarize_pop3()
{
	my (%server) = @_;

	my %samples;
	my %stats;

	my $server_str = "$server{user}\@$server{'host'}:$server{'port'}";

	#if(($flags{'debug'}) || ($flags{'verbose'})) {
	print "Checking mailbox: $server_str\n";
	#}

	my $socket;
	my $pop3;

	if($server{'port'} == 995) {	
		$socket = IO::Socket::SSL->new(
				Proto    => 'tcp',
				PeerAddr => $server{'host'},
				PeerPort => $server{'port'},
			) || die "Could not create socket on $server_str\n";

		$pop3 =	new Mail::POP3Client(
				USER     => $server{'user'},
				PASSWORD => $server{'pass'},
				HOST     => $server{'host'},
				SOCKET   => $socket,
				PEEK	 => 1,
			) || die "Could not open connection to $server_str\n";
	} else {

		$pop3 =	new Mail::POP3Client(
				USER     => $server{'user'},
				PASSWORD => $server{'pass'},
				HOST     => $server{'host'},
				PEEK	 => 1,
			) || die "Could not open connection to $server_str\n";
	}

	$stats{'1M'}	= 0;
	$stats{'5M'}	= 0;
	$stats{'10M'}	= 0;
	$stats{'15M'}	= 0;
	$stats{'20M'}	= 0;
	$stats{'25M'}	= 0;
	$stats{'count'}	= 0;
	$stats{'total'}	= 0;

	my @msgs = $pop3->List() or die "Could not list messages: $@\n";

	#print "Here are the first $flags{'num'} messages:\n";
        foreach my $message (@msgs) {
		
		my ($msg_num, $msg_size) = split(/\s+/, $message);

		if($stats{'count'} < $flags{'num'}) {
               		#print	"Message: $msg_num ",
				##$pop3->date($message), " ",
				#"$msg_size \n",
				##$pop3->subject($message), "\n";
				#$pop3->Head($msg_num), "\n";
		}
		$stats{'total'} += $msg_size;

		if($msg_size	<=	 1000000) { $stats{'1M'}++; }
		elsif($msg_size <=	 5000000) { $stats{'5M'}++; }
		elsif($msg_size <=	10000000) { $stats{'10M'}++; }
		elsif($msg_size <=	15000000) { $stats{'15M'}++; }
		elsif($msg_size <=	20000000) { $stats{'20M'}++; }
		elsif($msg_size <=	25000000) { $stats{'25M'}++; }
		else { ; }

		$stats{'count'}++;
        }
	$stats{'avg'} = $stats{'total'}/$stats{'count'};

	return (%stats, %samples);
}
#
# Subroutine:	write_summary
# Args:		%stats hash
# Return Value:	Prints summary for and returns 0 for success, 1 for error.
# Purpose:	Print the script's basic info to STDOUT. 
sub write_summary()
{
	my (%stats) = @_;

	print "1M -\t$stats{'1M'}\n";
	print "5M -\t$stats{'5M'}\n";
	print "10M -\t$stats{'10M'}\n";
	print "15M -\t$stats{'15M'}\n";
	print "20M -\t$stats{'20M'}\n";
	print "25M -\t$stats{'25M'}\n";
	print "total messages:\t$stats{'count'}\n";
	print "total size:\t$stats{'total'}\n";
	printf ("average size:\t%.2fM\n", $stats{'avg'}/1000000);
}
#
### EOF ###
