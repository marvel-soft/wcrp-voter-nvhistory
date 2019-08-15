#- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# wcrp-voter-nvvoter3
#  merge voter rolls with votestat, emails, etc
#
#
#
#- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
use strict;
use warnings;
$| = 1;
use File::Basename;
use DBI;
use Data::Dumper;
use Getopt::Long qw(GetOptions);
use Time::Piece;
use Math::Round;
use constant PROGNAME => "NVVOTER3 - ";
use Text::CSV qw( csv );

no warnings "uninitialized";

=head1 Function
=over
=head2 Overview
	This program will analyze a washoe-county-voter file
		a) file is sorted by precinct ascending
		b)
	Input: county voter registration file.
	       
	Output: a csv file containing the extracted fields 
=cut

my $records;

# primary input from sec state
my $inputFile = "VoterList.ElgbVtr.45099.073019143713.csv";
my $inputFileh;
my $baseFile = "base.csv";
my $baseFileh;
my %baseLine = ();
my @baseLine;
my $baseLine;
my @baseProfile;

# list of email addresses to add
my $voterEmailFile = "";
my $voterEmailFileh;
my @voterEmailArray;
my $voterEmailArray;
my @voterEmailHeadings;
my $voterEmailHeadings;

# email merge error report
my $emailLogFile = "email-adds-log.csv";
my $emailLogFileh;
my %emailLine = ();

#my $voterStatsFile = "voterdata.csv";

# sorted voter statistic records (by voterid)
my $voterStatsFile = "voterstat.csv";
my $voterStatsFileh;
my %voterStatsArray;
my @voterStatsArray;
my $voterStatsArray;
my $voterStatsHeadings = "";
my @voterStatsHeadings;

my $printFile = "print.txt";
my $printFileh;

my %politicalLine   = ();
my @adPoliticalHash = ();
my %adPoliticalHash;
my $adPoliticalHeadings = "";

my $stats;
my $emails;

my $helpReq     = 0;
my $maxLines    = "250";
my $voteCycle   = "";
my $fileCount   = 1;
my $csvHeadings = "";
my @csvHeadings;
my $line1Read    = '';
my $linesRead    = 0;
my $linesIncRead = 0;
my $printData;
my $linesWritten = 0;
my $emailAdded   = 0;
my $statsAdded   = 0;

my $skipRecords    = 20;
my $skippedRecords = 0;

my $generalCount;
my $party;
my $primaryCount;
my $pollCount;
my $absenteeCount   = 0;
my $activeVOTERS    = 0;
my $activeREP       = 0;
my $activeDEM       = 0;
my $activeOTHR      = 0;
my $totalVOTERS     = 0;
my $totalAMER       = 0;
my $totalAMEL       = 0;
my $totalDEM        = 0;
my $totalDUO        = 0;
my $totalFED        = 0;
my $totalGRN        = 0;
my $totalIA         = 0;
my $totalIAP        = 0;
my $totalIND        = 0;
my $totalINAC       = 0;
my $totalLIB        = 0;
my $totalLPN        = 0;
my $totalNL         = 0;
my $totalNP         = 0;
my $totalORGL       = 0;
my $totalOTH        = 0;
my $totalPF         = 0;
my $totalPOP        = 0;
my $totalREP        = 0;
my $totalRFM        = 0;
my $totalSOC        = 0;
my $totalTEANV      = 0;
my $totalUWS        = 0;
my $totalGENERALS   = 0;
my $totalPRIMARIES  = 0;
my $totalPOLLS      = 0;
my $totalABSENTEE   = 0;
my $totalSTRDEM     = 0;
my $totalMODDEM     = 0;
my $totalWEAKDEM    = 0;
my $percentSTRGRDEM = 0;
my $totalSTRREP     = 0;
my $totalMODREP     = 0;
my $totalWEAKREP    = 0;
my $percentSTRGREP  = 0;
my $totalSTROTHR    = 0;
my $totalMODOTHR    = 0;
my $totalWEAKOTHR   = 0;
my $percentSTRGOTHR = 0;
my $totalOTHR       = 0;

my @csvRowHash;
my %csvRowHash = ();
my @partyHash;
my %partyHash  = ();
my %schRowHash = ();
my @schRowHash;
my @values1;
my @values2;
my $voterRank;

my $calastName;
my $cafirstName;
my $camiddleName;
my $caemail;
my $capoints;

my $baseHeading = "";
my @baseHeading = (
    "CountyID",   "StateID",  "Status",      "Precinct",
    "AssmDist",   "SenDist",  "First",       "Last",
    "Middle",     "Suffix",   "Phone",       "email",
    "BirthDate",  "RegDate",  "Party",       "StreetNo",
    "StreetName", "Address1", "Address2",    "City",
    "State",      "Zip",      "RegDateOrig", "RegisteredDays",
    "Age",        "Generals", "Primaries",   "Polls",
    "Absentee",   "Mail",     "Provisional", "Rank",
    "Score",      "TotalVotes",
);
my @emailProfile;
my $emailHeading = "";
my @emailHeading = ( "VoterID", "Precinct", "First", "Last", "Middle", "email", );

my @votingLine;
my $votingLine;
my @votingProfile;

my $precinct = "000000";
my $noVotes = 0;

#
# main program controller
#
sub main {

    #Open file for messages and errors
    open( $printFileh, '>>' , "$printFile" )
      or die "Unable to open PRINT: $printFile Reason: $!";

    # Parse any parameters
    GetOptions(
        'infile=s'    => \$inputFile,
        'outile=s'    => \$baseFile,
        'statfile=s'  => \$voterStatsFile,
        'emailfile=s' => \$voterEmailFile,
        'skip=i'      => \$skipRecords,
        'lines=s'     => \$maxLines,
        'votecycle'   => \$voteCycle,
        'help!'       => \$helpReq,
    ) or die "Incorrect usage!\n";

    my $csv = Text::CSV->new(
        {
        binary             => 1,  # Allow special character. Always set this
        auto_diag          => 1,  # Report irregularities immediately
        allow_whitespace   => 0,
        allow_loose_quotes => 1,
        quote_space        => 0,
        }
    );

    if ($helpReq) {
        print "Come on, it's really not that hard.\n";
    }
    else {
        printLine("My inputfile is: $inputFile.\n");
    }
    unless ( open( $inputFileh, $inputFile ) ) {
        printLine("Unable to open INPUT: $inputFile Reason: $!\n");
        die;
    }

    # pick out the heading line and hold it
    $line1Read = $csv->getline ( $inputFileh );

    # move headings into an array to modify
    @csvHeadings = @$line1Read;
    # Remove spaces in headings
    my $j = @csvHeadings;
    for (my $i=0; $i < ($j-1); $i++) {
        $csvHeadings[$i] =~ s/\s//g;
    }

    # Build heading for new voter record
    $baseHeading = join( ",", @baseHeading );
    $baseHeading = $baseHeading . "\n";

    # Build heading for new voting record
    $emailHeading = join( ",", @emailHeading );
    $emailHeading = $emailHeading . "\n";

    # Initialize process loop and open files
    printLine("Voter Base-table file: $baseFile\n");
    open( $baseFileh, ">$baseFile" )
      or die "Unable to open baseFile: $baseFile Reason: $!";
    print $baseFileh $baseHeading;

    # initialize the voter email log and the email array
    if ( $voterEmailFile ne "" ) {
        printLine("My emailFile is: $voterEmailFile.\n");
        printLine("Email updates file: $voterEmailFile\n");
        open( $emailLogFileh, ">$emailLogFile" )
          or die "Unable to open emailLogFileh: $emailLogFile Reason: $!";
        print $emailLogFileh $emailHeading;
        voterEmailLoad(@voterEmailArray);
    }

    # if voter stats are available load the hash table
    if ( $voterStatsFile ne "" ) {
        printLine("Voter Stats file: $voterStatsFile\n");
        voterStatsLoad(@voterStatsArray);
    }

    #----------------------------------------------------------
    # Process loop
    # Read the entire input and
    # 1) edit the input lines
    # 2) transform the data
    # 3) write out transformed line
    #----------------------------------------------------------

  NEW:
    my $tmp = "";
    while ( $line1Read = $csv->getline( $inputFileh )  ) {
        $linesRead++;
        $linesIncRead++;
        if ( $linesIncRead == 10000 ) {
            printLine("$linesRead lines processed\r");
            $linesIncRead = 0;
        }

        # create the values array to complete preprocessing
        @values1 = @$line1Read;
        @csvRowHash{@csvHeadings} = @values1;

        #- - - - - - - - - - - - - - - - - - - - - - - - - -
        # Assemble database load  for base segment
        #- - - - - - - - - - - - - - - - - - - - - - - - - -
        %baseLine = ();
        $baseLine{"StateID"} = $csvRowHash{"VoterID"};
        my $voterid = $csvRowHash{"VoterID"};
        $baseLine{"CountyID"} = $csvRowHash{"CountyVoterID"};
        $baseLine{"Status"}   = $csvRowHash{"CountyStatus"};
        $baseLine{"Precinct"} = $csvRowHash{"RegisteredPrecinct"};
        $baseLine{'AssmDist'} = $csvRowHash{"AssemblyDistrict"};
        $baseLine{'SenDist'}  = $csvRowHash{"SenateDistrict"};

        # convert proper names to upper case first then lower
        my $UCword = $csvRowHash{"FirstName"};
        $UCword =~ s/(\w+)/\u\L$1/g;
        $baseLine{"First"} = $UCword;
        my $ccfirstName = $UCword;

        $UCword = $csvRowHash{"MiddleName"};
        $UCword =~ s/(\w+)/\u\L$1/g;
        $baseLine{"Middle"} = $UCword;
        $UCword = $csvRowHash{"LastName"};
        $UCword =~ s/(\w+)/\u\L$1/g;
        if ($UCword =~ m/,/) {
            $UCword = "\"" . $UCword . "\"";
        }
        $baseLine{"Last"} = $UCword;
        my $cclastName = $UCword;
        $UCword =~ s/(\w+)/\u\L$1/g;

        $baseLine{"BirthDate"} = $csvRowHash{"BirthDate"};
        $baseLine{"RegDate"}   = $csvRowHash{"RegistrationDate"};
        $baseLine{"Party"}     = $csvRowHash{"Party"};
        $baseLine{"Phone"}     = $csvRowHash{"Phone"};
        $UCword                = $csvRowHash{"Address1"};
        $UCword =~ s/(\w+)/\u\L$1/g;
        $baseLine{"Address1"} = $UCword;
        my @streetno = split( / /, $UCword, 2 );
        $baseLine{"StreetNo"}   = $streetno[0];
        $baseLine{"StreetName"} = $streetno[1];
        $UCword                 = $csvRowHash{"City"};
        $UCword =~ s/(\w+)/\u\L$1/g;
        $baseLine{"City"}  = $UCword;
        $baseLine{"State"} = $csvRowHash{"State"};
        $baseLine{"Zip"}   = $csvRowHash{"Zip"};
        $baseLine{"email"} = "";

        #
        #  locate and add voter statistics
        $stats = -1;
        $stats = binary_search( \@voterStatsArray, $voterid );

        #print " $voterid $stats \n";
        if ( $stats != -1 ) {
            $baseLine{"RegisteredDays"} = $voterStatsArray[$stats][3];
            $baseLine{"Age"}            = $voterStatsArray[$stats][2];
            $baseLine{"Generals"}       = $voterStatsArray[$stats][4];
            $baseLine{"Primaries"}      = $voterStatsArray[$stats][5];
            $baseLine{"Polls"}          = $voterStatsArray[$stats][6];
            $baseLine{"Absentee"}       = $voterStatsArray[$stats][7];
            $baseLine{"Mail"}           = $voterStatsArray[$stats][8];
            $baseLine{"Provisional"}    = $voterStatsArray[$stats][9];
            $baseLine{"Rank"}           = $voterStatsArray[$stats][10];
            $baseLine{"Score"}          = $voterStatsArray[$stats][11];
            $baseLine{"TotalVotes"}     = $voterStatsArray[$stats][12];
            $statsAdded                 = $statsAdded + 1;
            if ( $baseLine{"TotalVotes"} == 0) {
                $noVotes++;
            }
        }else {
            # fill in record for registered voter with no vote history
            $noVotes++;
            $baseLine{"RegisteredDays"} = 0;
            $baseLine{"Age"}            = 0;
            $baseLine{"Generals"}       = 0;
            $baseLine{"Primaries"}      = 0;
            $baseLine{"Polls"}          = 0;
            $baseLine{"Absentee"}       = 0;
            $baseLine{"Mail"}           = 0;
            $baseLine{"Provisional"}    = 0;
            $baseLine{"Rank"}           = "WEAK";
            $baseLine{"Score"}          = 0;
            $baseLine{"TotalVotes"}     = 0;
        }
#
#  locate email address
#  "Last", "First", "Middle","Phone","email","Address", "City","Contact Points",
#     0       1         2                4      5          6          7

        #if ( $voterEmailFile ne "" ) {
        $emails = binary_ch_search( \@voterEmailArray, $cclastName );
        if ( $emails != -1 ) {
            printLine("Email index = $emails not -1\n");
            if (   $voterEmailArray[$emails][0] eq $cclastName
                && $voterEmailArray[$emails][1] eq $ccfirstName )
            {
                $calastName        = $voterEmailArray[$emails][0];
                $cafirstName       = $voterEmailArray[$emails][1];
                $caemail           = $voterEmailArray[$emails][4];
                $baseLine{"email"} = $voterEmailArray[$emails][4];
                $capoints          = $voterEmailArray[$emails][7];
                $capoints =~ s/;/,/g;
                $emailAdded = $emailAdded + 1;

                # build a trace line to show email was updated
                %emailLine = ();
                $emailLine{"VoterID"} = $voterid;
                $emailLine{"Precinct"} = substr $csvRowHash{"precinct"}, 0, 6;
                $emailLine{"Last"}     = $calastName;
                $emailLine{"First"}    = $cafirstName;
                $emailLine{"email"}    = $caemail;
                @emailProfile          = ();

                foreach (@emailHeading) {
                    push( @emailProfile, $emailLine{$_} );
                }
                print $emailLogFileh join( ',', @emailProfile ), "\n";
            }
        }

        @baseProfile = ();
        foreach (@baseHeading) {
            push( @baseProfile, $baseLine{$_} );
        }
        print $baseFileh join( ',', @baseProfile ), "\n";

        $linesWritten++;
#        #
#        # For now this is the in-elegant way I detect completion
#        if ( eof(INPUT) ) {
#            goto EXIT;
#        }
#        next;
    }
    #
#    goto NEW;
}
#
# call main program controller
main();
#
# Common Exit
EXIT:

printLine("<===> Completed transformation of: $inputFile \n");
printLine("<===> BASE LOAD SEGMENTS available in file: $baseFile \n");
printLine("<===> Total Eligible Voter Records Read: $linesRead \n");
printLine("<===> Total Registered Voters with no Vote History: $noVotes\n");
printLine("<===> Total Email Addresses added: $emailAdded \n");
printLine("<===> Total Voting History Stats added: $statsAdded \n");
printLine("<===> Total base.csv Records written: $linesWritten \n");

close($inputFileh);
close($baseFileh);
close($printFileh);
if ( $voterEmailFile ne "" ) {
    close($emailLogFileh);
}
exit;

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
        print "$try \n";
    }
    $try = -1;
    return $try;             # The word isn't there.
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
    return $try;             # The word isn't there.
}

#
# count party memebers
#
sub countParty {
    $party = $csvRowHash{"party"};
    $totalVOTERS++;

    if ( $csvRowHash{"status"} eq "A" ) {
        $activeVOTERS++;
        if    ( $party eq 'REP' ) { $activeREP++; }
        elsif ( $party eq 'DEM' ) { $activeDEM++; }
        else                      { $activeOTHR++; }
    }
    if    ( $party eq 'AMEL' )  { $totalAMEL++; }
    elsif ( $party eq 'AMER' )  { $totalAMER++; }
    elsif ( $party eq 'DEM' )   { $totalDEM++; }
    elsif ( $party eq 'DUO' )   { $totalDUO++; }
    elsif ( $party eq 'FED' )   { $totalFED++; }
    elsif ( $party eq 'GRN' )   { $totalGRN++; }
    elsif ( $party eq 'IA' )    { $totalIA++; }
    elsif ( $party eq 'IAP' )   { $totalIAP++; }
    elsif ( $party eq 'IND' )   { $totalIND++; }
    elsif ( $party eq 'IN AC' ) { $totalINAC++; }
    elsif ( $party eq 'LIB' )   { $totalLIB++; }
    elsif ( $party eq 'LPN' )   { $totalLPN++; }
    elsif ( $party eq 'NL' )    { $totalNL++; }
    elsif ( $party eq 'NP' )    { $totalNP++; }
    elsif ( $party eq 'ORG L' ) { $totalORGL++; }
    elsif ( $party eq 'OTH' )   { $totalOTH++; }
    elsif ( $party eq 'PF' )    { $totalPF++; }
    elsif ( $party eq 'POP' )   { $totalPOP++; }
    elsif ( $party eq 'REP' )   { $totalREP++; }
    elsif ( $party eq 'RFM' )   { $totalRFM++; }
    elsif ( $party eq 'SOC' )   { $totalSOC++; }
    elsif ( $party eq 'TEANV' ) { $totalTEANV++; }
    elsif ( $party eq 'UWS' )   { $totalUWS++; }
}
#

#
# create the voter stats array that will be accessed via binary search
#
sub voterStatsLoad() {
    printLine("Started building Voter stats hash \n");

    my $loadCnt = 0;
    my $Scsv = Text::CSV->new(
        {
            binary             => 1,  # Allow special character. Always set this
            auto_diag          => 1,  # Report irregularities immediately
            allow_whitespace   => 0,
            allow_loose_quotes => 1,
            quote_space        => 0,
        }
    );

    $voterStatsHeadings = "";
    open( $voterStatsFileh, $voterStatsFile )
      or die "Unable to open INPUT: $voterStatsFile Reason: $!";

    $line1Read = $Scsv->getline ($voterStatsFileh); # get header 
    @voterStatsHeadings = @$line1Read;              # in voter Stats Headings Array
    # Build the UID->survey hash
    while ( $line1Read = $Scsv->getline( $voterStatsFileh ) ) {
        my @values1 = @$line1Read;
        push @voterStatsArray, \@values1;
        $loadCnt++;
    }
    close $voterStatsFileh;
    printLine("Completed building Voter stats hash for $loadCnt voters.\n");
    return @voterStatsArray;
}

#
# create the voter email binary search array
#
sub voterEmailLoad() {
    $voterEmailHeadings = "";
    open( $voterEmailFileh, $voterEmailFile )
      or die "Unable to open INPUT: $voterEmailFile Reason: $!";
    $voterEmailHeadings = <$voterEmailFileh>;
    chomp $voterEmailHeadings;
    printLine("Started Building email address array\n");

    # headings in an array to modify
    @voterEmailHeadings = split( /\s*,\s*/, $voterEmailHeadings );
    my $emailCount = 0;

    # Build the UID->survey hash
    while ( $line1Read = <$voterEmailFileh> ) {
        chomp $line1Read;
        my @values1 = split( /\s*,\s*/, $line1Read, -1 );
        push @voterEmailArray, \@values1;
        $emailCount = $emailCount + 1;
    }
    close $voterEmailFileh;
    printLine("Loaded email array: $emailCount entries");
    return @voterEmailArray;
}
#
# Print report line
#
sub printLine {
    my $datestring = localtime();
    ($printData) = @_;
    if ( substr( $printData , -1 ) ne "\r") {
        print $printFileh PROGNAME . $datestring . ' ' . $printData;
    }
    print( PROGNAME . $datestring . ' ' . $printData );
}
