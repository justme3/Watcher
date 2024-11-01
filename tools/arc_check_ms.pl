#!/usr/bin/perl

# Script: arc_check_ms.pl
# By: jvossler
# Dated: 
# Purpose: tool to identify problems with client mail sources

our $author		= 'jvossler';
our $author_email	= 'John_Vossler\@McAfee.com';
our $last_update	= '01-24-2009';
our $script_name	= 'arc_check_ms.pl';
our $script_version	= '0.1';

use strict;
#use warnings;

use lib '/usr/local/bin/opsadmin/perl/';

use DBI;
use Data::Dumper;
use Getopt::Long qw(:config no_ignore_case bundling);
use IO::Socket::SSL;
use Mail::IMAPClient;
use Mail::POP3Client;
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

	if(defined($flags{'host'})) {
		$args{'server_host'} = $flags{'host'};
	}
	if(defined($flags{'user'})) {
		$args{'username'} = $flags{'user'};
	}
	if(defined($flags{'port'})) {
		$args{'server_port'} = $flags{'port'};
	}

	my @sids = Arch::select_arc_mail_source(%args);

	foreach my $server_id (@sids) {

		my $mailsource = Arch::ms_from_sid($server_id);
		my %ms_details = %$mailsource;
		#my %ms_details = Arch::ms_from_sid($server_id);
		my $cust = MXL::customer_from_cid($ms_details{'id'});
		my %cust_details = %$cust;

		if(($flags{'debug'}) || ($flags{'verbose'})) {
			my $server_string =	sprintf (
							"%s\@%s:%s",
							$ms_details{'username'},
							$ms_details{'server_host'},
							$ms_details{'server_port'}
							);
			print "Server: $server_string ($server_id)\n";
			#print "Customer ID: $ms_details{'id'}\n";
			print "Customer: $cust_details{'name'} ($ms_details{'id'})\n";
		}

		my %ms = (
				'host'	=> $ms_details{'server_host'},
				'pass'	=> `echo $ms_details{'password'} | /mxl/bin/crypt -d`,
				'port'	=> $ms_details{'server_port'},
				#'proto'	=> 'pop3',
				#'ssl'	=> 1,
				'user'	=> $ms_details{'username'},
			);

		if($flags{'debug'}) {
			print "Mailsource Details: \n";
			print Dumper(\%ms); 
		}

		my (%stats, %samples);
		my $status;

		if($ms_details{'server_port'} == 110) { 
			eval { (%stats, %samples) = &summarize_pop3(%ms) }; 
			if($@) { $status = $@; warn "Warning: $status"; } else {undef $status; }
		} elsif($ms_details{'server_port'} == 143) { 
			eval { (%stats, %samples) = &summarize_imap(%ms) };
			if($@) { $status = $@; warn "Warning: $status"; } else {undef $status; }
		} elsif($ms_details{'server_port'} == 993) { 
			eval { (%stats, %samples) = &summarize_imap(%ms) };
			if($@) { $status = $@; warn "Warning: $status"; } else {undef $status; }
		} elsif($ms_details{'server_port'} == 995) { 
			eval { (%stats, %samples) = &summarize_pop3(%ms) };
			if($@) { $status = $@; warn "Warning: $status"; } else {undef $status; }
		} else { ; }
	
		if($flags{'count'}) { 
			if(defined($status)) { print "ERROR: $status"; }
			else { print "$stats{'count'}\n"; }
		} elsif($flags{'total'}) { 
			if(defined($status)) { print "ERROR: $status"; }
			else { print "$stats{'total'}\n"; }
		} else {
			&write_summary(%stats);
		}
		#eval { my $status = &summarize_imap(%ms) }; warn $@ if $@;
		#eval { my $status = &summarize_pop3(%ms) }; warn $@ if $@;
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
			'count'		=> 0,
			'help'		=> 0,
			'verbose'	=> 0,
			'version'	=> 0,
		);

	my $result = GetOptions (	
					"c"		=> \$hash{'count'},	# binary
					"count"		=> \$hash{'count'},	# binary
					"d+"		=> \$hash{'debug'},	# incremental
					"f"		=> \$hash{'folders'},	# binary
					"folders"	=> \$hash{'folders'},	# binary
					"debug+"	=> \$hash{'debug'},	# incremental
					"h"		=> \$hash{'help'},	# binary
					"help"		=> \$hash{'help'},	# binary
					"host=s"	=> \$hash{'host'},	# string
					'n=i',		=> \$hash{'num'},	# integer
					"port=s"	=> \$hash{'port'},	# string
					"t"		=> \$hash{'total'},	# binary
					"total"		=> \$hash{'total'},	# binary
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
# Purpose:	Write the appropriate usage of mikes_metrics.pl to STDOUT. 
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
	my $imap;
	my $socket;
	my $server_str = "$server{user}\@$server{'host'}:$server{'port'}";

	unless(($flags{'count'}) || ($flags{'total'})) {
		print "Checking mailbox: $server_str\n";
	}

	if($server{'port'} == 993) {
		if($flags{'debug'}) { print "Using IMAPS (993)\n"; }
		$socket = IO::Socket::SSL->new(
				Proto    => 'tcp',
				PeerAddr => $server{'host'},
				PeerPort => $server{'port'},
				Timeout  => 90,
			) || die "Could not create socket on $server_str\n";

		$imap = Mail::IMAPClient->new(
				Password	=> $server{'pass'},
				Peek		=> 1,
				Socket  	=> $socket,
				User		=> $server{'user'},
			) or die "Could not connect to $server_str\n";
	} else {
		if($flags{'debug'}) { print "Using IMAP (143)\n"; }
		$imap = Mail::IMAPClient->new(
				Server		=> $server{'host'},
				Peek		=> 1,
			) or die "Could not connect to IMAP $server_str\n";

# perform STARTTLS if necessary

			my %capabilities = map { uc($_) => 1 } $imap->capability();
			if ($capabilities{'STARTTLS'} && $capabilities{'LOGINDISABLED'})
			{
				if($flags{'debug'}) { print "Promoting socket with STARTTLS\n"; }
				$imap->starttls();
			}

# perform LOGIN or AUTHENTICATE

			$imap->User($server{'user'});
			$imap->Password($server{'pass'});
			$imap->login()
				or die "Could not authenticate to IMAP $server_str\n";

	}

	$stats{'total'}	= 0;
	$stats{'count'}	= 0;

	if($flags{'count'} > 0) {
		$stats{'count'} = $imap->message_count('INBOX');
	} else {

		#$imap->Timeout(10);
		$imap->select('INBOX') || die "Could not select INBOX on $server_str\n";
	
		#my @folders = $imap->folders();
		#foreach my $folder (@folders) {
		#	if($folder eq 'INBOX') {
		#		print "Folder: $folder exists!\n";
		#	}
		#}
	
		my @msgs = $imap->messages() or die "Could not list messages for $server_str\n";
	
	        foreach my $message (@msgs) {
	
			my $size = $imap->size($message);
	
			#if($stats{'count'} < $flags{'num'}) { ; }
			
			unless(defined($size)) { next; }
			
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
	}

	if ($flags{'folders'}) {

		my @folders = $imap->folders
        		or die "List folders error: ", $imap->LastError, "\n";
		print "Folders: \n";
		foreach my $folder (@folders) {
			print "\t$folder";
			if($flags{'count'}) {
				my $count=$imap->message_count($folder);
				print " ($count)";
			}
			print "\n";
		}

	}

	if($stats{'count'} == 0) {
		$stats{'avg'} = 0;
	} else {
		$stats{'avg'} = $stats{'total'}/$stats{'count'};
	}

		if($flags{'debug'}) { 
			print "Return from sub\n";
			print Dumper \%stats ;
			print Dumper \%samples ;
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

	unless(($flags{'count'}) || ($flags{'total'})) {
		print "Checking mailbox: $server_str\n";
	}

	my $socket;
	my $pop3;

	if($server{'port'} == 995) {	
		if($flags{'debug'}) { print "Using POPS (995)\n"; }
		$socket = IO::Socket::SSL->new(
				Proto    => 'tcp',
				PeerAddr => $server{'host'},
				PeerPort => $server{'port'},
				Timeout  => 30,
			) || die "Could not create socket on $server_str\n";

		$pop3 =	new Mail::POP3Client(
				USER     => $server{'user'},
				PASSWORD => $server{'pass'},
				HOST     => $server{'host'},
				SOCKET   => $socket,
				PEEK	 => 1,
				TIMEOUT  => 10,
			) || die "Could not open connection to $server_str\n";
	} else {
		if($flags{'debug'}) { print "Using POP (110)\n"; }
		$pop3 =	new Mail::POP3Client(
				USER     => $server{'user'},
				PASSWORD => $server{'pass'},
				HOST     => $server{'host'},
				PEEK	 => 1,
				TIMEOUT  => 10,
			) || die "Could not open connection to $server_str\n";
	}

	$stats{'count'}	= 0;
	$stats{'total'}	= 0;

	if($flags{'count'} > 0) {
		$stats{'count'} = $pop3->Count(); 
	} else {

		my @msgs = $pop3->List() or die "Could not list pop3 messages for $server_str\n";
	
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
	
			unless(defined($msg_num) && defined($msg_size)) { next; }
	
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
	}

	if ($flags{'folders'}) {

		my @folders = $pop3->folders
        		or die "List folders error: $!\n";
		print "Folders: \n";
		foreach my $folder (@folders) {
			print "\t$folder";
			if($flags{'count'}) {
				my $count=$pop3->Count($folder);
				print " ($count)";
			}
			print "\n";
		}
	}

	if($stats{'count'} == 0) {
		$stats{'avg'} = 0;
	} else {
		$stats{'avg'} = $stats{'total'}/$stats{'count'};
	}

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
	
	unless(defined($stats{'1M'}))	{ $stats{'1M'} = 0; };
	unless(defined($stats{'5M'}))	{ $stats{'5M'} = 0; };
	unless(defined($stats{'10M'}))	{ $stats{'10M'} = 0; };
	unless(defined($stats{'15M'}))	{ $stats{'15M'} = 0; };
	unless(defined($stats{'20M'}))	{ $stats{'20M'} = 0; };
	unless(defined($stats{'25M'}))	{ $stats{'25M'} = 0; };
	unless(defined($stats{'count'})) { $stats{'count'} = 0; };
	unless(defined($stats{'total'})) { $stats{'total'} = 0; };
	unless(defined($stats{'avg'}))	{ $stats{'avg'} = 0; };

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
