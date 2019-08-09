#- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# wcrp-voter-nvhistory
#  -- nvvoter1
#  Convert the NVSOS voter data to voter Statistic lines
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
use Text::CSV qw( csv );
use constant PROGNAME => "NVVOTER1 - ";

no warnings "uninitialized";

=head1 Function
=over
=head2 Overview

=cut

my $records;

#my $inputFile = "nvsos-voter-history-test-noheading.csv";

# input file 1
my $voterHistoryFile = "VoterList.VtHst.45099.073019143713.csv";
#my $voterHistoryFile = "test1.vthist.voter-5.csv";
my $voterHistoryFileh;
my @voterHistoryLine = ();
my %voterHistoryLine;

# outut voter records
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
my $csvRowHash;
my @csvRowHash;
my %csvRowHash   = ();
my $stateVoterID = 0;
my @date;
my $adjustedDate;
my $before;
my $vote;
my $cycle;
my $totalVotes      = 0;
my $linesIncRead    = 0;
my $linesIncWritten = 0;
my $currentVoter;

# this header array will need modification after and election cycle
my @voterDataHeading = (
    "statevoterid",
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
    "TotalVotes ",
);

#
# main program controller
#
sub main {

    # Open file for messages and errors
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

    # prepare to use text::csv module
    # build the constructor
    my $csv = Text::CSV->new(
        {
            binary             => 1,  # Allow special character. Always set this
            auto_diag          => 1,  # Report irregularities immediately
            allow_whitespace   => 0,
            allow_loose_quotes => 1,
            quote_space        => 0,
        }
    );
    @csvHeadings    = $csv->header($voterHistoryFileh);
    # on input these two column headers contained a space - replace headers
    $csvHeadings[2] = "electiondate";
    $csvHeadings[3] = "votecode";
    $csv->column_names(@csvHeadings);

    # Build heading for new voting record
    $voterDataHeading = join( ",", @voterDataHeading );
    $voterDataHeading = $voterDataHeading . "\n";
    open( $voterDataFileh, ">$voterDataFile" )
      or die "Unable to open base: $voterDataFile Reason: $! \n";
    print $voterDataFileh $voterDataHeading;

    #
    # Initialize process loop and open first output
    $linesRead = 0;

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
    while ( $line1Read = $csv->getline_hr($voterHistoryFileh) ) {
        $linesRead++;
        if ( $linesIncRead == 10000 ) {
            printLine("$linesRead lines processed \n");
            print "$linesRead lines processed \n";
            $linesIncRead = 0;
        }

        # then create the values array for the record
        %csvRowHash = %{$line1Read};

        # - - - - - - - - - - - - - - - - - - - - - - - - -
        # Create hash of line for transformation
        # - - - - - - - - - - - - - - - - - - - - - - - - -
        # for first record of a series for a voter
        $currentVoter = $csvRowHash{"voterid"};
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
      next_voter:
        if ( $currentVoter eq $stateVoterID ) {
            $voterDataLine{"statevoterid"} = $csvRowHash{"voterid"};
            #
            # place vote in correct bucket (14 days <= electiondate)
            #
            my $votedate = substr( $csvRowHash{"electiondate"}, 0, 10 );
            my $vdate    = Time::Piece->strptime( $votedate, "%m/%d/%Y" );

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
                      $csvRowHash{votecode};
                    $totalVotes++;
                    last;
                }
            }
            next;
        }
        else {
            $voterDataLine{"TotalVotes"} = $totalVotes;

            # prepare to write out the voter data
            @voterData = ();
            foreach (@voterDataHeading) {
                push( @voterData, $voterDataLine{$_} );
            }
            print $voterDataFileh join( ',', @voterData ), "\n";
            %voterDataLine = ();
            $linesWritten++;
            $linesIncWritten++;
            $totalVotes = 0;
            $linesRead++;
            if ( $linesIncWritten == 1000 ) {
                printLine ("$linesWritten lines written \n");
                $linesIncWritten = 0;
            }
            # set voterid for next record
            $stateVoterID = $currentVoter;
            goto next_voter;
        }

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

printLine("<===> Completed processing: $voterHistoryFile \n");
printLine("<===> Created voter output: $voterDataFile \n");
printLine("<===> Total Records Read:   $linesRead \n");
printLine("<===> Total Records written: $linesWritten \n");

close($printFileh);
exit;

#
# Print report line
#
sub printLine {
    my $datestring = localtime();
    ($printData) = @_;
    print $printFileh PROGNAME . $datestring . ' ' . $printData;
    print (PROGNAME . $datestring . ' ' . $printData);
}
