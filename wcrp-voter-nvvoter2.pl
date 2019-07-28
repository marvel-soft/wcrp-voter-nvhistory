#- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# wcrp-voter-nvhistory
#  Convert the voter rows to voter statistics
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

my $voterDataFile = "voterdata.csv";
my $voterDataFileh;
my @voterData;

my $voterStatFile = "voterstat.csv";
my $voterStatFileh;

my $printFile = "print-.txt";
my $printFileh;

my $helpReq = 0;

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

my %voterStatLine = ();
my @voterStatLine;
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
    "null4",             #11
    "null5",             #12
    "null6",             #13
    "Rank",              #14
    "gender",            #15
    "military",          #16
);
my @voterStat;

my @precinctPolitical;
my $absenteeCount  = 0;
my $activeVOTERS   = 0;
my $activeREP      = 0;
my $activeDEM      = 0;
my $activeOTHR     = 0;
my $totalVOTERS    = 0;
my $totalGENERALS  = 0;
my $totalPRIMARIES = 0;
my $totalPOLLS     = 0;
my $totalABSENTEE  = 0;
my $totalSTR = 0;
my $totalMOD = 0;
my $totalWEAK = 0;

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
        'outfile=s'  => \$voterStatFile,
        'maxlines=n' => \$maxLines,
        'maxfiles=n' => \$maxFiles,
        'help!'      => \$helpReq,

    ) or die "Incorrect usage! \n";
    if ($helpReq) {
        print "Come on, it's really not that hard. \n";
    }
    else {
        printLine("My inputfile is: $voterDataFile. \n");
    }
    unless ( open( INPUT, $voterDataFile ) ) {
        printLine("Unable to open INPUT: $voterDataFile Reason: $! \n");
        die;
    }

    # pick out the heading line and hold it and remove end character
    $csvHeadings = <INPUT>;
    chomp $csvHeadings;
    chop $csvHeadings;

    # headings in an array to modify
    # @csvHeadings will be used to create the files
    @csvHeadings = split( /\s*,\s*/, $csvHeadings );

    # Build heading for new statistics record
    $voterStatHeading = join( ",", @voterStatHeading );
    $voterStatHeading = $voterStatHeading . "\n";
    open( $voterStatFileh, ">$voterStatFile" )
      or die "Unable to open output: $voterStatFile Reason: $! \n";
    print $voterStatFileh $voterStatHeading;

    #
    # Initialize process loop and open first output
    $linesRead = 0;

#- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# main process loop.
#
#  for each of several records in input convert to csvRowHash row for unique voter
#    - currentVoter = record-id
#    - analyze the votes and create a voter score
#    - create the voterstat record
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
        # for first record
        # clear all election date buckets to blanks
        for ( $cycle = 1 ; $cycle < 20 ; $cycle++ ) {
            $voterStatLine{ $voterStatHeading[$cycle] } = " ";
        }
        $voterStatLine{"state-voter-id"} = $csvRowHash{"state-voter-id"};
        evaluateVoter();
        $voterStatLine{"Primaries"} = $primaryCount;
		$voterStatLine{"Generals"}  = $generalCount;
		$voterStatLine{"Polls"}     = $pollCount;
		$voterStatLine{"Absentee"}  = $absenteeCount;
		$voterStatLine{"Rank"}  = $voterRank;



        # write out voter stats
        @voterStat = ();
        foreach (@voterStatHeading) {
            push( @voterStat, $voterStatLine{$_} );
        }
        print $voterStatFileh join( ',', @voterStat ), "\n";
        %voterStatLine = ();
        $linesWritten++;
        $cycle = 1;
    }

    #
    # For now this is the in-elegant way I detect completion

    if ( eof(INPUT) ) {
        goto EXIT;
    }
    next;
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

#  routine: evaluateVoter
#
# determine if reliable voter by voting pattern over last five cycles
# tossed out special elections and mock elections
#  voter reg_date is considered
#  weights: strong, moderate, weak
# if registered < 2 years       gen >= 1 and pri <= 0   = STRONG
# if registered > 2 < 4 years   gen >= 1 and pri >= 0   = STRONG
# if registered > 4 < 8 years   gen >= 4 and pri >= 0   = STRONG
# if registered > 8 years       gen >= 6 and pri >= 0   = STRONG
#
sub evaluateVoter {
    my $generalPollCount  = 0;
    my $generalEarlyCount = 0;
    my $generalNotVote    = 0;
    my $notElegible       = 0;
    my $primaryPollCount  = 0;
    my $primaryEarlyCount = 0;
    my $primaryNotVote    = 0;
    $generalCount  = 0;
    $primaryCount  = 0;
    $pollCount     = 0;
    $absenteeCount = 0;
    $voterRank     = '';

    #set first vote in list
    my $vote = 2;
    my $cyc;

    #my $daysRegistered = $newLine{"Days Registered"};
    for ( my $cycle = 1 ; $cycle < 20 ; $cycle++, $vote += 1 ) {
        $cyc = $cycle;

        #skip mock election
        if ( ( $csvHeadings[$vote] ) =~ m/mock/ ) {
            next;
        }

# each election type is specified with its date - we only process primary/general
#skip special election
        if ( ( $csvHeadings[$vote] ) =~ m/special/ ) {
            next;
        }

        #skip sparks election
        if ( ( $csvHeadings[$vote] ) =~ m/sparks/ ) {
            next;
        }
        #
        # record a general vote
        # if there is no vote recorded shown with a "blank" then NOT ELEGIBLE
        #
        if ( ( $csvHeadings[$vote] ) =~ m/general/ ) {
            if ( $csvRowHash{ $csvHeadings[$vote] } eq ' ' ) {
                $notElegible += 1;
                next;
            }
            if ( $csvRowHash{ $csvHeadings[$vote] } eq ' ' ) {
                $notElegible += 1;
                next;
            }
            #
            # the following vote codes are supported
            # - EV early vote
            # - FW federal write in
            # - MB mail ballot
            # - PP polling place
            # - PV provisional vote
            #
            if ( $csvRowHash{ $csvHeadings[$vote] } eq 'N' ) {
                $generalNotVote += 1;
                next;
            }
            if ( $csvRowHash{ $csvHeadings[$vote] } eq 'PP' ) {
                $generalPollCount += 1;
                $generalCount     += 1;
                $pollCount        += 1;
                next;
            }
            if ( $csvRowHash{ $csvHeadings[$vote] } eq 'EV' ) {
                $generalEarlyCount += 1;
                $generalCount      += 1;
                $absenteeCount     += 1;
                next;
            }
        }
        #
        # record a primary vote
        # if there is no vote recorded shown with a "blank" then NOT ELEGIBLE
        #
        if ( ( $csvHeadings[$vote] ) =~ m/primary/ ) {
            if ( $csvRowHash{ $csvHeadings[$vote] } eq ' ' ) {
                $notElegible += 1;
                next;
            }
            if ( $csvRowHash{ $csvHeadings[$vote] } eq 'N' ) {
                $primaryNotVote += 1;
                next;
            }
            if ( $csvRowHash{ $csvHeadings[$vote] } eq 'PP'  ) {
                $primaryPollCount += 1;
                $primaryCount     += 1;
                $pollCount        += 1;
                next;
            }
            if ( $csvRowHash{ $csvHeadings[$vote] } eq 'EV'  ) {
                $primaryEarlyCount += 1;
                $primaryCount      += 1;
                $absenteeCount     += 1;
            }
        }
    }

  # Likely voter score:
  # if registered < 2 years       gen <= 1 || notelig >= 1            = WEAK
  # if registered < 2 years       gen == 1 ||                         = MODERATE
  # if registered < 2 years       gen == 2 ||                         = STRONG

  # if registered > 2 < 4 years   gen <= 0 || notelig >= 1            = WEAK
  # if registered > 2 < 4 years   gen >= 2 && pri >= 0                = MODERATE
  # if registered > 2 < 4 years   gen >= 3 && pri >= 1                = STRONG

  # if registered > 4 < 8 years   gen >= 0 || notelig >= 1            = WEAK
  # if registered > 4 < 8 years   gen >= 0 && gen <= 2  and pri == 0  = WEAK
  # if registered > 4 < 8 years   gen >= 2 && gen <= 5  and pri >= 0  = MODERATE
  # if registered > 4 < 8 years   gen >= 3 && gen <= 12 and pri >= 0  = STRONG

  # if registered > 8 years   gen >= 0 && gen <= 2 || notelig >= 1    = WEAK
  # if registered > 8 years   gen >= 0 && gen <= 4  and pri == 0      = WEAK
  # if registered > 8 years   gen >= 3 && gen <= 9  and pri >= 0      = MODERATE
    ## if registered > 8 years   gen >= 6 && gen <= 12 and pri >= 0      = STRONG

    if ( $daysTotlRegistered < ( 365 * 2 + 1 ) ) {
        if ( $generalCount <= 1 or $notElegible >= 1 ) {
            $voterRank = "WEAK";
        }
        if ( $generalCount >= 1 ) {
            $voterRank = "MODERATE";
        }
        if ( $generalCount >= 2 ) {
            $voterRank = "STRONG";
        }
    }

    # if registered > 2 years and < 4 years>
    if (    $daysTotlRegistered > ( 365 * 2 )
        and $daysTotlRegistered < ( 365 * 4 ) )
    {
        if ( $generalCount == 0 or $generalCount == 1 or $notElegible >= 1 ) {
            $voterRank = "WEAK";
        }
        if ( $generalCount >= 2 ) {
            $voterRank = "MODERATE";
        }
        if ( $generalCount >= 3 and $primaryCount >= 1 ) {
            $voterRank = "STRONG";
        }
    }

    # if registered > 4 < 8 years   gen gt 4 && pri gt 3   = STRONG
    if (    $daysTotlRegistered > ( 365 * 4 )
        and $daysTotlRegistered < ( 365 * 8 ) )
    {
        if ( $generalCount >= 0 or $notElegible >= 1 ) {
            $voterRank = "WEAK";
        }
        if ( $generalCount >= 1 and $generalCount <= 2 and $primaryCount = 0 ) {
            $voterRank = "WEAK";
        }
        if ( $generalCount >= 2 and $generalCount <= 5 and $primaryCount >= 0 )
        {
            $voterRank = "MODERATE";
        }
        if ( $generalCount >= 3 and $generalCount <= 12 and $primaryCount >= 0 )
        {
            $voterRank = "STRONG";
        }
    }

    # if registered > 8 years       gen gt 6 && pri gt 4   = STRONG
    if ( $daysTotlRegistered > ( 365 * 8 ) ) {
        if ( $generalCount >= 0 and $generalCount <= 2 or $notElegible >= 1 ) {
            $voterRank = "WEAK";
        }
        if ( $generalCount >= 0 and $generalCount <= 4 and $primaryCount >= 0 )
        {
            $voterRank = "WEAK";
        }
        if (    $generalCount >= 3
            and $generalCount <= 9
            and $primaryCount >= 0 )
        {
            $voterRank = "MODERATE";
        }
        if ( $generalCount >= 6 and $generalCount <= 12 and $primaryCount >= 0 )
        {
            $voterRank = "STRONG";
        }
    }
    #
    # Set voter strength rating
    #
    if    ( $voterRank eq 'STRONG' )   { $totalSTR++; }
    elsif ( $voterRank eq 'MODERATE' ) { $totalMOD++; }
    elsif ( $voterRank eq 'WEAK' )     { $totalWEAK++; }
    

      if ( $primaryCount != 0 ) {
        if ( $leansDemCount != 0 ) {
            if ( $leansDemCount / $primaryCount > .5 ) {
                $leanDem = 1;
            }
        }
        if ( $leansRepCount != 0 ) {
            if ( $leansRepCount / $primaryCount > .5 ) {
                $leanRep = 1;
            }
        }
    }

    $totalGENERALS  = $totalGENERALS + $generalCount;
    $totalPRIMARIES = $totalPRIMARIES + $primaryCount;
    $totalPOLLS     = $totalPOLLS + $pollCount;
    $totalABSENTEE  = $totalABSENTEE + $absenteeCount;
    $totalLEANREP   = $totalLEANREP + $leanRep;
    $totalLEANDEM   = $totalLEANDEM + $leanDem;
}
