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
	'infile=s'     => \$inputFile,
	'outfile=s'    => \$voterDataFile,
	'maxlines=s'   => \$maxLines,
	'maxfiles=n'   => \$maxFiles,
	'help!'        => \$helpReq,
	
=cut

my $records;
#my $inputFile = "nvsos-voter-history-test.csv";
my $inputFile = "vote-history-20190509.csv";

my $fileName = "";

my $baseFile = "base.csv";
my $baseFileh;
my $voterDataFile = "voterdata.csv";
my $voterDataFileh;
my @voterData;

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

my $voterStatHeading = "";
my @voterStatHeading = (
    "state-voter-id",    #0
    "Voter Status",      #1
    "Precinct",          #2
    "Last Name",         #3
    "null1",             #4
    "null2",             #5
    "null3",             #6
    "Generals",          #7
    "Primaries",         #8
    "Polls",             #9
    "Absentee",          #10
    "LeansDEM",          #11
    "LeansREP",          #12
    "Leans",             #13
    "Rank",              #14
    "gender",            #15
    "military",          #16

);

my $voterStatFile = "voterstat.csv";
my $voterStatFileh;
my %voterStatLine = ();

my $voterDataHeading = "";
my %voterDataLine    = ();
my @voterDataLine;
my @voterDataHeading = (
    "state-voter-id",
    "11/06/18 general",
    "06/12/18 primary",
    "11/08/16 general",
    "06/14/16 primary",
    "11/04/14 general",
    "06/10/14 primary",
    "11/06/12 general",
    "06/12/12 primary",
    "09/13/11",
    "11/02/10 general",
    "06/08/10 primary",
    "11/04/08 general",
    "08/12/08 primary",
    "11/07/06 general",
    "08/05/06 primary",
    "11/02/04 general",
    "09/07/04 primary",
    "06/03/03",
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
        'infile=s'   => \$inputFile,
        'outfile=s'  => \$voterDataFile,
        'maxlines=n' => \$maxLines,
        'maxfiles=n' => \$maxFiles,
        'help!'      => \$helpReq,

    ) or die "Incorrect usage! \n";
    if ($helpReq) {
        print "Come on, it's really not that hard. \n";
    }
    else {
        printLine("My inputfile is: $inputFile. \n");
    }
    unless ( open( INPUT, $inputFile ) ) {
        printLine("Unable to open INPUT: $inputFile Reason: $! \n");
        die;
    }

    # pick out the heading line and hold it and remove end character
    $csvHeadings = <INPUT>;
    chomp $csvHeadings;
    chop $csvHeadings;

    # headings in an array to modify
    # @csvHeadings will be used to create the files
    @csvHeadings = split( /\s*,\s*/, $csvHeadings );

    # Build heading for new voting record
    $voterDataHeading = join( ",", @voterDataHeading );
    $voterDataHeading = $voterDataHeading . "\n";
    open( $voterDataFileh, ">$voterDataFile" )
      or die "Unable to open base: $voterDataFile Reason: $! \n";
    print $voterDataFileh $voterDataHeading;

    # Build heading for new statistics record
    $voterStatHeading = join( ",", @voterStatHeading );
    $voterStatHeading = $voterStatHeading . "\n";
    open( $voterStatFileh, ">$voterStatFile" )
      or die "Unable to open output: $voterStatFile Reason: $! \n";
    print $voterStatFileh $voterStatHeading;

    #
    # Initialize process loop and open first output
    $linesRead = 0;
    my $currentVoter;

#- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# main process loop.
#  initialize program
#
#  for each of several records in input convert to csvRowHash row for unique voter
#    - currentVoter = record-id
#    - get record from input
#    - if currentVoter is same as stateVoterID then add segment to row
#      else write-the-row,
#      stateVoterID = currentVoter
#   endloop
#
#- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

  NEW:
    while ( $line1Read = <INPUT> ) {
        $linesRead++;
        if ( $linesIncRead == 10000 ) {
            printLine("$linesRead lines processed \n");
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
        $currentVoter = $csvRowHash{"voter-id"};
        if ( $stateVoterID == 0 ) {
            $stateVoterID  = $currentVoter;
            %voterDataLine = ();

            #$cycle         = 1;
            # clear all election date buckets to blanks
            for ( $cycle = 1 ; $cycle < 20 ; $cycle++ ) {
                $voterDataLine{ $voterDataHeading[$cycle] } = " ";
            }
        }

        # for all records build a line for each voter with all their
        # votes by election
      finish:
        if ( $currentVoter eq $stateVoterID ) {
            $voterDataLine{"state-voter-id"} = $csvRowHash{"voter-id"};
            #
            # place vote in correct bucket (14 days <= electiondate)
            #
            my $votedate = $csvRowHash{"election-date"};
            my $vdate    = Time::Piece->strptime( $votedate, "%m/%d/%y" );
            my $vote;

            # find the correct election for this vote
            # dates must be in Time::Piece format
            for ( my $cycle = 1, $vote = 1 ;
                $cycle < 20 ; $cycle++, $vote += 1 )
            {
                # create the earlydate used for testing the votedate
                my $edate         = $voterDataHeading[$vote];
                my $electiondate  = Time::Piece->strptime( $edate, "%m/%d/%y" );
                my $twoweeksearly = $electiondate - 2 * ONE_WEEK;
                my $nowdate       = $twoweeksearly->mdy;

                # test to find if the votedate fits a slot, add the vote
                if ( $vdate >= $twoweeksearly && $vdate <= $electiondate ) {
                    $voterDataLine{ $voterDataHeading[$vote] } =
                      $csvRowHash{"vote-type"};
                    last;
                }
            }

            #   $cycle++;
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
        if ( eof(INPUT) ) {
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

close(INPUT);
close($voterDataFileh);
close($voterStatFileh);

printLine("<===> Completed processing of: $inputFile \n");
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
