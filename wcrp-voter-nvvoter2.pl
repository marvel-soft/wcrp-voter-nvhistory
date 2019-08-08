#- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# wcrp-voter-nvhistory
#  --nvvoter2
#  Convert the voter data rows to voter statistics
#   - requires the file voterValues as supplemental input
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
use DateTime;
use Time::Piece;
use Time::Seconds;
use Math::Round;
use Text::CSV qw( csv );
use constant PROGNAME => "NVVOTER2 - ";

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

# input 1 from voter0
my $voterValuesFile = "votervalues-s.csv";
my $voterValuesFileh;
my @voterValuesArray;

# inupt 2 from voter1
my $voterDataFile = "voterdata.csv";
my $voterDataFileh;
my @voterData;

# output file to voter3
my $voterStatFile = "voterstat.csv";
my $voterStatFileh;
my %voterStatLine = ();
my @voterStatLine;
my @voterStat;

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
my %csvRowHash = ();
my @date;
my $voterid;
my $adjustedDate;

my $voterStatHeading = "";
my @voterStatHeading = (
    "statevoterid",      #0
    "VoterStatus",       #1
    "Age",               #2
    "RegisteredDays",    #3
    "Generals",          #4
    "Primaries",         #5
    "Polls",             #6
    "Absentee",          #7
    "Mail",              #8
    "Provisional",       #9
    "Rank",              #10
    "Score",             #11
    "TotalVotes",        #12
);

my @precinctPolitical;
my $RegisteredDays   = 0;
my $pollCount        = 0;
my $absenteeCount    = 0;
my $provisionalCount = 0;
my $mailCount        = 0;
my $activeVOTERS     = 0;
my $activeREP        = 0;
my $activeDEM        = 0;
my $activeOTHR       = 0;
my $totalVOTERS      = 0;
my $totalGENERALS    = 0;
my $totalPRIMARIES   = 0;
my $totalPOLLS       = 0;
my $totalABSENTEE    = 0;
my $totalPROVISIONAL = 0;
my $totalMAIL        = 0;
my $totalSTR         = 0;
my $totalMOD         = 0;
my $totalWEAK        = 0;
my $votesTotal       = 0;
my $voterScore       = 0;
my $voterScore2      = 0;
#
# main program controller
#
sub main {

    #Open file for messages and errors
    open( $printFileh, ">$printFile" )
      or die "Unable to open PRINT: $printFile Reason: $!";

    # Parse any parameters
    GetOptions(
        'infile=s'   => \$voterDataFile,
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
    unless ( open( $voterDataFileh, $voterDataFile ) ) {
        printLine("Unable to open INPUT: $voterDataFile Reason: $! \n");
        die;
    }

    # pick out the heading line and hold it and remove end character
    $csvHeadings = <$voterDataFileh>;
    chomp $csvHeadings;

    #chop $csvHeadings;

    # remove imbedded commas and imbedded spaces from headers
    $csvHeadings =~ s/(?:\G(?!\A)|[^"]*")[^",]*\K(?:,|"(*SKIP)(*FAIL))/ /g;
    $csvHeadings =~ s/(?<! ) (?! )//g;

    # headings in an array to modify
    @csvHeadings = split( /\s*,\s*/, $csvHeadings );

    # Build heading for new statistics record
    $voterStatHeading = join( ",", @voterStatHeading );
    $voterStatHeading = $voterStatHeading . "\n";
    open( $voterStatFileh, ">$voterStatFile" )
      or die "Unable to open output: $voterStatFile Reason: $! \n";
    print $voterStatFileh $voterStatHeading;

    # if voter stats are available load the hash table
    if ( $voterValuesFile ne "" ) {
        printLine("Voter values file: $voterValuesFile\n");
        &voterValuesLoad(@voterValuesArray);
    }

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
    while ( $line1Read = <$voterDataFileh> ) {
        $linesRead++;
        if ( $linesIncRead == 10000 ) {
            printLine("$linesRead lines processed \n");
            $linesIncRead = 0;
        }

        # replace commas from in between double quotes with a space
        chomp $line1Read;

        #chop $line1Read;
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
        #
        #  locate county data
        #
        $voterid = $csvRowHash{statevoterid};
        $stats   = -1;
        $stats   = binary_search( \@voterValuesArray, $voterid );
        if ( $stats != -1 ) {
            $voterStatLine{"Precinct"} = $voterValuesArray[$stats][1];
            $voterStatLine{"LastName"} = $voterValuesArray[$stats][2];

            #$voterStatLine{"Birthdate"} = $voterValuesArray[$stats][3];
            my $birthdate = $voterValuesArray[$stats][3];

            #$voterStatLine{"Reg-Date"} = $voterValuesArray[$stats][4];
            my $regdate = $voterValuesArray[$stats][4];
            $voterStatLine{"Party"}       = $voterValuesArray[$stats][5];
            $voterStatLine{"VoterStatus"} = $voterValuesArray[$stats][6];

            # determine age
            my ( @date, $yy, $mm, $dd, $now, $age, $regdays );
            @date = split( /\s*\/\s*/, $birthdate, -1 );

            $mm = sprintf( "%02d", $date[0] );
            $dd = sprintf( "%02d", $date[1] );
            $yy = sprintf( "%02d", substr( $date[2], 0, 4 ) );

            # if    ( $yy <= 20 ) { $yy = 2000 + $yy }
            # elsif ( $yy > 20 )  { $yy = 1900 + $yy }
            $adjustedDate = "$mm/$dd/$yy";
            $before       = Time::Piece->strptime( $adjustedDate, "%m/%d/%Y" );
            $now          = localtime;
            $age          = $now - $before;
            $age          = ( $age / (86400) / 365 );
            $age          = round($age);
            $voterStatLine{"Age"} = $age;

            # determine registered days
            # may get dates in two formats: mm/dd/yyyy or yyyy-mm-dd
            if ( substr( $regdate, 4, 1 ) eq '-' ) {

                # handle yyyy-mm-dd (ISO-8898)
                @date = split( /\s*\-\s*/, $regdate, -1 );
                $mm   = $date[1];
                $dd   = $date[2];
                $yy   = $date[0];
            }
            else {
                # handle mm/dd/yyyy
                @date = split( /\s*\/\s*/, $regdate, -1 );
                $mm   = sprintf( "%02d", $date[0] );
                $dd   = sprintf( "%02d", $date[1] );
                $yy   = sprintf( "%02d", substr( $date[2], 0, 4 ) );
            }

            if ( $yy < 1900 ) {
                $yy = 2016;
            }
            $adjustedDate = "$mm/$dd/$yy";

            $before  = Time::Piece->strptime( $adjustedDate, "%m/%d/%Y" );
            $now     = localtime;
            $regdays = $now - $before;
            $regdays = ( $regdays / (86400) );
            $regdays = round($regdays);
            $voterStatLine{"RegisteredDays"} = $regdays;
            $statsAdded = $statsAdded + 1;
        }

        $voterStatLine{"statevoterid"} = $csvRowHash{"statevoterid"};
        $voterid = $csvRowHash{"statevoterid"};

        # evaluate voter strength
        evaluateVoter();

        #build remainder of line
        $voterStatLine{"Primaries"}   = $primaryCount;
        $voterStatLine{"Generals"}    = $generalCount;
        $voterStatLine{"Polls"}       = $pollCount;
        $voterStatLine{"Absentee"}    = $absenteeCount;
        $voterStatLine{"Mail"}        = $mailCount;
        $voterStatLine{"Provisional"} = $provisionalCount;
        $voterStatLine{"Rank"}        = $voterRank;
        $voterStatLine{"Score"}       = $voterScore2;
        $voterStatLine{"TotalVotes"}  = $votesTotal;

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

    if ( eof($voterDataFileh) ) {
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

close($voterDataFileh);
close($voterStatFileh);
close($voterValuesFileh);

printLine("<===> Completed processing of: $voterDataFile \n");
printLine("<===> Completed creation of: $voterStatFile \n");
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
    print $printFileh PROGNAME . $datestring . ' ' . $printData;
    print( PROGNAME . $datestring . ' ' . $printData );
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
    $generalCount     = 0;
    $primaryCount     = 0;
    $pollCount        = 0;
    $absenteeCount    = 0;
    $mailCount        = 0;
    $provisionalCount = 0;
    $votesTotal       = 0;
    $voterRank        = '';

    #set pointer to first vote in list
    my $vote = 2;

    my $daysRegistered = $voterStatLine{"Registered Days"};
    for ( my $cycle = 1 ; $cycle < 20 ; $cycle++, $vote += 1 ) {

# each election type is specified with its date - we only process primary/general
# skip mock election
        if ( ( $csvHeadings[$vote] ) =~ m/mock/ ) {
            next;
        }

        # skip special election
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
                $votesTotal = $vote;
                next;
            }
            if ( $csvRowHash{ $csvHeadings[$vote] } eq 'EV' ) {
                $generalEarlyCount += 1;
                $generalCount      += 1;
                $absenteeCount     += 1;
                $votesTotal = $vote;
                next;
            }
            if ( $csvRowHash{ $csvHeadings[$vote] } eq 'MB' ) {
                $generalEarlyCount += 1;
                $generalCount      += 1;
                $mailCount         += 1;
                $votesTotal = $vote;
                next;
            }
            if ( $csvRowHash{ $csvHeadings[$vote] } eq 'PV' ) {
                $generalCount     += 1;
                $provisionalCount += 1;
                $votesTotal = $vote;
                next;
            }
        }

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
            if ( $csvRowHash{ $csvHeadings[$vote] } eq 'PP' ) {
                $primaryPollCount += 1;
                $primaryCount     += 1;
                $pollCount        += 1;
                $votesTotal = $vote;
                next;
            }
            if ( $csvRowHash{ $csvHeadings[$vote] } eq 'EV' ) {
                $primaryEarlyCount += 1;
                $primaryCount      += 1;
                $absenteeCount     += 1;
                $votesTotal = $vote;
                next;
            }
            if ( $csvRowHash{ $csvHeadings[$vote] } eq 'MB' ) {
                $primaryEarlyCount += 1;
                $primaryCount      += 1;
                $mailCount         += 1;
                $votesTotal = $vote;
                next;
            }
            if ( $csvRowHash{ $csvHeadings[$vote] } eq 'PV' ) {
                $primaryCount     += 1;
                $provisionalCount += 1;
                $votesTotal = $vote;
                next;
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

    if ( $votesTotal > 0 ) {
        $voterScore  = ( $generalCount + $primaryCount ) / ($votesTotal) * 10;
        $voterScore2 = round($voterScore);
    }

    if ( $voterScore2 > 5 ) {
        $voterRank = "STRONG";
    }
    if ( $voterScore2 >= 3 && $voterScore2 <= 5 ) {
        $voterRank = "MODERATE";
    }
    if ( $voterScore2 < 3 ) {
        $voterRank = "WEAK";
    }

    #
    # Set voter strength rating
    #
    if    ( $voterRank eq 'STRONG' )   { $totalSTR++; }
    elsif ( $voterRank eq 'MODERATE' ) { $totalMOD++; }
    elsif ( $voterRank eq 'WEAK' )     { $totalWEAK++; }

    $totalGENERALS    = $totalGENERALS + $generalCount;
    $totalPRIMARIES   = $totalPRIMARIES + $primaryCount;
    $totalPOLLS       = $totalPOLLS + $pollCount;
    $totalABSENTEE    = $totalABSENTEE + $absenteeCount;
    $totalPROVISIONAL = $totalPROVISIONAL + $provisionalCount;
}

# $index = binary_search( \@array, $word )
#   @array is a list of lowercase strings in alphabetical order.
#   $word is the target word that might be in the list.
#   binary_search() returns the array index such that $array[$index]
#   is $word.
sub binary_search {
    my ( $try,   $var );
    my ( $array, $word ) = @_;
    my ( $low,   $high ) = ( 0, @$array - 1 );
    while ( $low <= $high ) {    # While the window is open
        $try = int( ( $low + $high ) / 2 );    # Try the middle element
        $var = $array->[$try][0];
        $low  = $try + 1, next if $array->[$try][0] < $word;    # Raise bottom
        $high = $try - 1, next if $array->[$try][0] > $word;    # Lower top
        return $try;    # We've found the word!
    }
    $try = -1;
    return;             # The word isn't there.
}
#
# binay search for character strings
#
sub binary_ch_search {
    my ( $try,   $var );
    my ( $array, $word ) = @_;
    my ( $low,   $high ) = ( 0, @$array - 1 );
    while ( $low <= $high ) {    # While the window is open
        $try = int( ( $low + $high ) / 2 );    # Try the middle element
        $var = $array->[$try][0];
        $low  = $try + 1, next if $array->[$try][0] lt $word;    # Raise bottom
        $high = $try - 1, next if $array->[$try][0] gt $word;    # Lower top
        return $try;    # We've found the word!
    }
    $try = -1;
    return;             # The word isn't there.
}
#
# create the voter stats binary search array
#
sub voterValuesLoad() {
    printLine("Starting to load voter values array \n");
    my $valuescounter = 0, $incCounter = 0;
    $voterValuesHeadings = "";
    open( $voterValuesFileh, $voterValuesFile )
      or die "Unable to open INPUT: $voterValuesFile Reason: $!";
    $voterValuesHeadings = <$voterValuesFileh>;
    chomp $voterValuesHeadings;

    # headings in an array to modify
    @voterValuesHeadings = split( /\s*,\s*/, $voterValuesHeadings );

    # Build the UID->survey hash
    while ( $line1Read = <$voterValuesFileh> ) {
        chomp $line1Read;

        #   write "values $line1Read  \n";
        my @values1 = split( /\s*,\s*/, $line1Read, -1 );
        push @voterValuesArray, \@values1;
        $valuescounter++;
        $incCounter++;
        if ( $incCounter == 1000 ) {
            $incCounter = 0;
        }

    }
    printLine("Completed voterValuesArray for $valuescounter \n");
    close $voterValuesFileh;
    return @voterValuesArray;
}
