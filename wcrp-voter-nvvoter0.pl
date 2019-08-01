#- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# wcrp-voter-nvhistory
#  Create nv voter extracts with key values used to create
#  voter stats
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
	This program will create voter pre-extracts
		a) no restrictions
		b)
	Input: NVSOS eligible voter file
	       
	Output: one or more smaller csv file
	parms:
	'infile=s'     => \$inputFile,
	'outfile=s'    => \$voterValueFile,
	'maxlines=s'   => \$maxLines,
	'maxfiles=n'   => \$maxFiles,
	'help!'        => \$helpReq,
	
=cut

my $records;

#my $inputFile = "nvsos-voter-history-test-noheading.csv";

my $inputFile = "VoterList.Elgbvtr.100.csv";

my $voterValueFile = "votervalues.csv";
my $voterValueFileh;
my @voterValuesLine = ();
my %voterValuesLine;
my @voterValues;

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
my %csvRowHash = ();

my $voterValuesHeading = "";
my @voterValuesHeading = (
    "state-voter-id",    #0
    "Precinct",          #2
    "LastName",          #3
    "Birthdate",         #4
    "Reg-Date",          #5
    "Party",             #6
    "Status",            #7
);

# main program controller
#
sub main {

    #Open file for messages and errors
    open( $printFileh, ">$printFile" )
      or die "Unable to open PRINT: $printFile Reason: $!";

    # Parse any parameters
    GetOptions(
        'infile=s'   => \$inputFile,
        'outfile=s'  => \$voterValueFile,
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

    # remove imbedded commas and imbedded spaces from headers
    $csvHeadings =~ s/(?:\G(?!\A)|[^"]*")[^",]*\K(?:,|"(*SKIP)(*FAIL))/ /g;
    $csvHeadings =~ s/(?<! ) (?! )//g;

    # headings in an array to modify
    # @csvHeadings will be used to create the files
    @csvHeadings = split( /\s*,\s*/, $csvHeadings );

    # Build heading for new voting record
    $voterValuesHeading = join( ",", @voterValuesHeading );
    $voterValuesHeading = $voterValuesHeading . "\n";
    open( $voterValueFileh, ">$voterValueFile" )
      or die "Unable to open base: $voterValueFile Reason: $! \n";
    print $voterValueFileh $voterValuesHeading;

    #
    # Initialize process loop and open first output
    $linesRead = 0;

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
        if ( $linesIncRead == 100 ) {
            print STDOUT "$linesRead lines processed \n";

            $linesIncRead = 0;
        }

        # replace commas from in between double quotes with a space
        chomp $line1Read;
        $line1Read =~ s/(?:\G(?!\A)|[^"]*")[^",]*\K(?:,|"(*SKIP)(*FAIL))/ /g;

        # then create the values array to complete preprocessing
        @values1 = split( /\s*,\s*/, $line1Read, -1 );
        @csvRowHash{@csvHeadings} = @values1;

        # - - - - - - - - - - - - - - - - - - - - - - - - -
        # Create hash of line for transformation
        # - - - - - - - - - - - - - - - - - - - - - - - - -
        # for first record of a series for a voter

        # votes by election

        $voterValuesLine{"state-voter-id"} = $csvRowHash{"VoterID"};
        $voterValuesLine{"Party"}          = $csvRowHash{"Party"};
        $voterValuesLine{"LastName"}       = $csvRowHash{"LastName"};
        $voterValuesLine{"Precinct"}       = $csvRowHash{"RegisteredPrecinct"};
        my @date = split( /\s*\/\s*/, $csvRowHash{"BirthDate"}, -1 );
        $mm = sprintf( "%02d", $date[0] );
        $dd = sprintf( "%02d", $date[1] );
        $yy = sprintf( "%02d", $date[2] );
        $voterValuesLine{"Birthdate"} = "$mm/$dd/$yy";
        @date = split( /\s*\/\s*/, $csvRowHash{"RegistrationDate"}, -1 );
        $mm   = sprintf( "%02d", $date[0] );
        $dd   = sprintf( "%02d", $date[1] );
        $yy   = sprintf( "%02d", $date[2] );
        $voterValuesLine{"Reg-Date"} = "$mm/$dd/$yy";
        $voterValuesLine{"Status"}   = $csvRowHash{"CountyStatus"};

        # prepare to write out the voter data
        @voterValues = ();
        foreach (@voterValuesHeading) {
            push( @voterValues, $voterValuesLine{$_} );
        }
        print $voterValueFileh join( ',', @voterValues ), "\n";
        %voterValuesLine = ();
        $linesWritten++;
        goto NEW;
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
close($voterValueFileh);

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
