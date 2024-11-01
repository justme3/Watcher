#!/usr/bin/perl -w

use strict;
use warnings;

require '/usr/local/bin/opsadmin/perl/MXL/Arch.pm';
require '/usr/local/bin/opsadmin/perl/MXL/MXL.pm';

sub main();
sub get_opts();
sub usage();

our %flags = ();

&main();

sub main() {

	&get_opts();

	my $status = 0;
	my $nagtext = '';

	my %result_cust;
	my %result_errs;
	my %result_msgs;

	my @cids = Arch::cids_from_mail_source();

	if($flags{'debug'}){ print "Searching $#cids customers.\n"; }
	
	foreach my $cid (@cids) {
		
		my $cust_ref = MXL::customer_from_cid($cid);
		my %customer = %$cust_ref; 
		my @mailers = Arch::sids_from_cid($cid);

		if($flags{'debug'}){ print "Searching customer $customer{'name'}.\n"; }

		foreach my $sid (@mailers) {

			my $ms_ref = Arch::ms_from_sid($sid);
			my %ms_hash = %$ms_ref;
			my $unique_host = "$ms_hash{'username'}\@$ms_hash{'server_host'}:$ms_hash{'server_port'}";
			my $cust_str = "$customer{'name'} ($cid)"; 

			if($flags{'debug'}){ print "Searching mailsource $unique_host.\n"; }

			my $cmd =	"/usr/local/bin/opsadmin/arc_check_ms.pl -c ".
					"--host $ms_hash{'server_host'} ".
					"--user $ms_hash{'username'} ".
					"--port $ms_hash{'server_port'}";

			my $count = `$cmd`; chomp($count);

			$result_cust{$unique_host} = $cust_str;
			$result_msgs{$unique_host} = $count;

			if($result_msgs{$unique_host} =~ 'Could not connect') 
			{
				delete($result_msgs{$unique_host});
				$result_errs{$unique_host}  = 'Connection problem!';
			}
			elsif($result_msgs{$unique_host} =~ 'Could not create') 
			{
				delete($result_msgs{$unique_host});
				$result_errs{$unique_host} = 'Socket problem!    ';
			} 
			elsif($result_msgs{$unique_host} =~ 'Could not list') 
			{
				delete($result_msgs{$unique_host});
				$result_errs{$unique_host} = 'No message list!';
			} 
			else 
			{ 
				; 
			}
		}
	}

	print "--- Archive Mailsource Backlog Report ---\n";
	foreach my $unique_host( sort {$result_msgs{$b} <=> $result_msgs{$a} } keys %result_msgs ) {
		print "$result_msgs{$unique_host} - $unique_host - $result_cust{$unique_host}\n";
	}
	print "\nThe following mailsources had errors:\n";
	foreach my $unique_host( sort keys %result_errs ) {
		print "$result_errs{$unique_host} - $unique_host - $result_cust{$unique_host}\n";
	}
}

sub get_opts()
{
	use Getopt::Long qw(:config no_ignore_case bundling);
        Getopt::Long::Configure("bundling");
        GetOptions(
                'error|e=s'	=> \$flags{'error'},
                'warn|w=s'	=> \$flags{'warn'},
                'debug|d'	=> \$flags{'debug'},
                'remove|r'	=> \$flags{'remove'},
                'verbose|v'	=> \$flags{'verbose'},
                'nagios|n'	=> \$flags{'nagios'},
                'quiet|q'	=> \$flags{'quiet'},
                'help|usage|h'	=> sub {warn &usage; exit 1;})
                        or die &usage;

        $flags{'debug'} || ($flags{'debug'} = 0);
        $flags{'verbose'} || ($flags{'verbose'} = 0);
        $flags{'nagios'} || ($flags{'nagios'} = 0);
        $flags{'quiet'} || ($flags{'quiet'} = 0);

	if($flags{'quiet'}) {
		$flags{'verbose'} = 0;
		$flags{'nagios'} = 0;
		$flags{'debug'} = 0;
	} elsif($flags{'nagios'}) {
		$flags{'debug'} = 0;
		$flags{'verbose'} = 0;
	} else { ; }
	
}
# Subroutine:   usage
# Args:         <void>
# Return Value: <void>
# Purpose:      Write the appropriate usage to STDOUT.
sub usage()
{
my $usage = <<EOF;
Usage: $0 [OPTIONS]

    *** NOTE: This script must be run as the mxl-archive user. ***

    -d, --debug               Debug Mode
    -h, --help                Print this help
EOF

        print $usage;
}
#
### EOF ###
