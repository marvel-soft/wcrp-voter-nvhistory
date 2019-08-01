#- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# wcrp-voter-nvhistory
#  Convert the NVSOS voter data to voterStat lines
#
#
#- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
#use strict;
use warnings;
$| = 1;
use File::Basename;
use DBI;
use Data::Dumper;
use Getopt::Long qw(GetOptions);
use Time::Piece;
use Time::Seconds;
use Math::Round;

no warnings "uninitialized";

=head1 Function
=over
=head2 Overview
	This program will create voter extracts
		a) no restrictions
		b)
	Input: any csv file with headers
	       
	Output: one or more smaller csv file
	parms:
	'infile=s'     => \$voterHistoryFileFile,
	'outfile=s'    => \$voterDataFile,
	'maxlines=s'   => \$maxLines,
	'maxfiles=n'   => \$maxFiles,
	'help!'        => \$helpReq,
	
=cut

my $records;

#my $inputFile = "nvsos-voter-history-test-noheading.csv";

my $voterHistoryFile = "VoterList.VtHist.100.csv";
my $voterHistoryFileh;
my @voterHistoryLine = ();
my %voterHistoryLine;

my $voterDataHeading = "";
my $voterDataFile    = "voterdata.csv";
my $voterDataFileh;
my @voterData;
my %voterDataLine = ();
my @voterDataLine;

my $printFile = "print-.txt";
my $printFileh;

my $helpReq   = 0;
my $fileCount = 0;

my $csvHeadings = "";
my @csvHeadings;
my $line1Read = '';
my $linesRead = 0;
my $printData;
my $linesWritten = 0;
my $maxFiles;
my $maxLines;
my @values1;
my @csvRowHash;
my %csvRowHash   = ();
my $stateVoterID = 0;
my @date;
my $adjustedDate;
my $before;
my $vote;
my $cycle;

my @voterDataHeading = (
    "state-voter-id",
    "11/06/18 general",
    "06/12/18 special",
    "11/08/16 general",
    "06/14/16 primary",
    "11/04/14 general",
    "06/10/14 primary",
    "11/06/12 general",
    "06/12/12 primary",
    "09/13/11 special",
    "11/02/10 general",
    "06/08/10 primary",
    "11/04/08 general",
    "08/12/08 primary",
    "11/07/06 general",
    "08/05/06 primary",
    "11/02/04 general",
    "09/07/04 primary",
    "06/03/03 special",
    "11/05/02 general",
    "09/03/02 primary",
);

#
# main program controller
#
sub main {

    #Open file for messages and errors
    open( $printFileh, ">$printFile" )
      or die "Unable to open PRINT: $printFile Reason: $!";

    # Parse any parameters
    GetOptions(
        'infile=s'   => \$voterHistoryFile,
        'outfile=s'  => \$voterDataFile,
        'maxlines=n' => \$maxLines,
        'maxfiles=n' => \$maxFiles,
        'help!'      => \$helpReq,

    ) or die "Incorrect usage! \n";
    if ($helpReq) {
        print "Come on, it's really not that hard. \n";
    }
    else {
        printLine("My voterHistoryFile is: $voterHistoryFile. \n");
    }
    unless ( open( $voterHistoryFileh, $voterHistoryFile ) ) {
        printLine(
            "Unable to open voterHistoryFileh: $voterHistoryFile Reason: $! \n"
        );
        die;
    }

    # pick out the heading line and hold it and remove end character
    $csvHeadings = <$voterHistoryFileh>;

    chomp $csvHeadings;
    chop $csvHeadings;
    
    # remove imbedded commas and imbedded spaces from headers
    $csvHeadings =~ s/(?:\G(?!\A)|[^"]*")[^",]*\K(?:,|"(*SKIP)(*FAIL))/ /g;
    $csvHeadings =~ s/(?<! ) (?! )//g;

    # headings in an array to modify
    # @csvHeadings will be used to create the files
    @csvHeadings = split( /\s*,\s*/, $csvHeadings );

    # Build heading for new voting record
    $voterDataHeading = join( ",", @voterDataHeading );
    $voterDataHeading = $voterDataHeading . "\n";
    open( $voterDataFileh, ">$voterDataFile" )
      or die "Unable to open base: $voterDataFile Reason: $! \n";
    print $voterDataFileh $voterDataHeading;

    #
    # Initialize process loop and open first output
    $linesRead = 0;
    my $currentVoter;

#- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# main process loop.
#  initialize program
#
#  for each of several records in voterHistoryFileh convert to csvRowHash row for unique voter
#    - currentVoter = record-id
#    - get record from voterHistoryFileh
#    - if currentVoter is same as stateVoterID then add segment to row
#      else write-the-row,
#      stateVoterID = currentVoter
#   endloop
#
#- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

  NEW:
    while ( $line1Read = <$voterHistoryFileh> ) {
        $linesRead++;
        if ( $linesIncRead == 100 ) {
            print STDOUT "$linesRead lines processed \n";

            # printLine("$linesRead lines processed \n");
            $linesIncRead = 0;
        }

        # replace commas from in between double quotes with a space
        chomp $line1Read;
        chop $line1Read;
        $line1Read =~ s/(?:\G(?!\A)|[^"]*")[^",]*\K(?:,|"(*SKIP)(*FAIL))/ /g;

        # then create the values array to complete preprocessing
        @values1 = split( /\s*,\s*/, $line1Read, -1 );
        @csvRowHash{@csvHeadings} = @values1;

        # - - - - - - - - - - - - - - - - - - - - - - - - -
        # Create hash of line for transformation
        # - - - - - - - - - - - - - - - - - - - - - - - - -
        # for first record of a series for a voter
        $currentVoter = $csvRowHash{"VoterID"};
        if ( $stateVoterID == 0 ) {
            $stateVoterID  = $currentVoter;
            %voterDataLine = ();

            # clear all election date buckets to blanks
            for ( $cycle = 1 ; $cycle < 20 ; $cycle++ ) {
                $voterDataLine{ $voterDataHeading[$cycle] } = " ";
            }
        }

        # for all records build a line for each voter with all their
        # votes by election
      finish:
        if ( $currentVoter eq $stateVoterID ) {
            $voterDataLine{"state-voter-id"} = $csvRowHash{"VoterID"};
            #
            # place vote in correct bucket (14 days <= electiondate)
            #
            my $votedate = substr( $csvRowHash{"ElectionDate"}, 0, 8 );
            my $vdate    = Time::Piece->strptime( $votedate, "%m/%d/%y" );

            # find the correct election for this vote
            # dates must be in Time::Piece format
            for ( $cycle = 1, $vote = 1 ; $cycle < 20 ; $cycle++, $vote += 1 ) {

                # create the earlydate used for testing the votedate
                my $edate         = substr( $voterDataHeading[$vote], 0, 8 );
                my $electiondate  = Time::Piece->strptime( $edate, "%m/%d/%y" );
                my $twoweeksearly = $electiondate - 2 * ONE_WEEK;
                my $nowdate       = $twoweeksearly->mdy;

                # test to find if the votedate fits a slot, add the vote
                if ( $vdate >= $twoweeksearly && $vdate <= $electiondate ) {
                    $voterDataLine{ $voterDataHeading[$vote] } =
                      $csvRowHash{"VoteCode"};
                    last;
                }
            }
            next;
        }
        else {
            # prepare to write out the voter data
            @voterData = ();
            foreach (@voterDataHeading) {
                push( @voterData, $voterDataLine{$_} );
            }
            print $voterDataFileh join( ',', @voterData ), "\n";
            %voterDataLine = ();
            $linesWritten++;
        }
        $stateVoterID = $currentVoter;
        goto finish;

        #
        # For now this is the in-elegant way I detect completion
        if ( eof(voterHistoryFileh) ) {
            goto EXIT;
        }
        next;
    }
}

#
# call main program controller
main();
#
# Common Exit
EXIT:

close(voterHistoryFileh);
close($voterDataFileh);

printLine("<===> Completed processing of: $voterHistoryFile \n");
printLine("<===> Total Records Read: $linesRead \n");
printLine("<===> Total Records written: $linesWritten \n");

close($printFileh);
exit;

#
# Print report line
#
sub printLine {
    my $datestring = localtime();
    ($printData) = @_;
    print $printFileh $datestring . ' ' . $printData;
    print $datestring . ' ' . $printData;
}
