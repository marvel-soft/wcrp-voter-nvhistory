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

#my $voterHistoryFile = "VoterList.VtHst.45099.073019143713.csv";
my $voterHistoryFile = "VoterList.VtHst.45099.073019143713.csv";
#my $voterHistoryFile = "test1.vthist.voter-5.csv";
my $voterHistoryFileh;
my @voterHistoryLine = ();
my %voterHistoryLine;

my $voterDataHeading = "";
my $voterDataFile    = "voterdata.csv";
my $voterDataFileh;
my @voterData;
my %voterDataLine = ();
my @voterDataLine;

my $printFile = "print.txt";
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
my $ignored = 0;
my $currentVoter;

my @voterDataHeading = (
    "statevoterid",
    "11/06/18 general",
    "06/12/18 primary",
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
    "08/15/06 primary",
    "11/02/04 general",
    "09/07/04 primary",
    "06/03/03 special",
    "11/05/02 general",
    "09/03/02 primary",
    "TotalVotes ",
);

my @voterHeadingDates =(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0 );
my @voterEarlyDates =(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0 );

#
# main program controller
#
sub main {

    # Open file for messages and errors
    open( $printFileh, ">>" , "$printFile" )
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
    $csvHeadings[0] = "uniquevoteid";
    $csvHeadings[1] = "voterid";
    $csvHeadings[2] = "electiondate";
    $csvHeadings[3] = "votecode";
    $csv->column_names(@csvHeadings);

    # Build heading for new voting record
    $voterDataHeading = join( ",", @voterDataHeading );
    $voterDataHeading = $voterDataHeading . "\n";
    open( $voterDataFileh, ">$voterDataFile" )
      or die "Unable to open base: $voterDataFile Reason: $! \n";
    print $voterDataFileh $voterDataHeading;

##

#  initialize oldest election date we care about
#
    my $string = $voterDataHeading[20];
    my $oldestElection = substr( $string, 0, 8 );
    my $oldestDate = Time::Piece->strptime( $oldestElection, "%m/%d/%y" );
    printLine("Oldest Election Date: $oldestDate\n");
#
# initialize binary election date arrays
#
    for ( $vote = 1; $vote <= 20; $vote++) {
        my $edate         = substr( $voterDataHeading[$vote], 0, 8 );
        my $electiondate  = Time::Piece->strptime( $edate, "%m/%d/%y" );
        $voterHeadingDates[$vote] = $electiondate;
        $voterEarlyDates[$vote] = ($electiondate - ONE_WEEK) - ONE_WEEK;
    }
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

    #my @cols = @{ $csv->getline($voterHistoryFileh) };
    #$row = {};
    #$csv->bind_columns( \@{$row}{@cols} );

    while ( $line1Read = $csv->getline_hr($voterHistoryFileh) ) {
        $linesRead++;
        $linesIncRead += 1;
 #       if ( $linesIncRead >= int( 10000) ) {
 #           printLine("$linesRead lines read \n");
 #           $linesIncRead = 0;
 #       }

        # then create the values array to complete preprocessing
        %csvRowHash = %{$line1Read};

        # - - - - - - - - - - - - - - - - - - - - - - - - -
        # Create hash of line for transformation
        # - - - - - - - - - - - - - - - - - - - - - - - - -
        # for first record of a series for a voter
        $currentVoter = $csvRowHash{"voterid"};
        if ($currentVoter == 4596250 ) {
            printLine("Input Line: $csvRowHash{'voterid'},$csvRowHash{'electiondate'},$csvRowHash{'votecode'}\n");
        }
        if ( $stateVoterID == 0 ) {
            $stateVoterID  = $currentVoter;
            %voterDataLine = ();

            # clear all election date buckets to blanks
            for ( $cycle = 0 ; $cycle <= 20 ; $cycle++ ) {
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
            if ( $vdate < $oldestDate ) {
                # ignore records for elections older than we are looking for
                $ignored +=1;
                next;
            }
            my $baddate = 1;

            # find the correct election for this vote
            # dates must be in Time::Piece format
            for ( $cycle = 1, $vote = 1 ; $cycle <= 20 ; $cycle++, $vote += 1 ) {

                # create the earlydate used for testing the votedate
                my $electiondate  = $voterHeadingDates[$vote];
                my $twoweeksearly = $voterEarlyDates[$vote];
                my $nowdate       = $twoweeksearly->mdy;

                # test to find if the votedate fits a slot, add the vote
                if ( $vdate >= $twoweeksearly && $vdate <= $electiondate ) {
                    $voterDataLine{ $voterDataHeading[$vote] } = $csvRowHash{votecode};
                    $totalVotes++;
                    $baddate = 0;
                    last;
                }
            }
            if ( $baddate !=0 ) {
                printLine("Unknown Election Date  $csvRowHash{'electiondate'}  for voter $csvRowHash{'voterid'} \n");
            }
            next;
        }
        else {
            if ( $voterDataLine[0] eq " ") {
                next;                           # we ignored all records for a voter, don't write anything
            }
            $voterDataLine{ $voterDataHeading[21] } = $totalVotes;
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
            if ( $linesIncWritten == 2000 ) {
                printLine ("$linesWritten lines written \r");
                $linesIncWritten = 0;
            }
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

printLine("<===> Completed processing of: $voterHistoryFile \n");
printLine("<===> Completed creation of  : $voterDataFile \n");
printLine("<===> Total Records Read: $linesRead \n");
printLine("<===> Total Records written: $linesWritten \n");
printLine("<===> Total Old Vote Records Ignored: $ignored \n");

close($printFileh);
exit;

#
# Print report line
#
sub printLine {
    my $datestring = localtime();
    ($printData) = @_;
    if ( substr( $printData , -1 ) ne "\r") {
        print $printFileh PROGNAME . $datestring . ' ' . $printData;
    }
    print (PROGNAME . $datestring . ' ' . $printData);
}
