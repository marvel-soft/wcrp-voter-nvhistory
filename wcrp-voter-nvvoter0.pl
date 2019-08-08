#- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# wcrp-voter-nvhistory
#  -- nvvoter0
#  Create nv votervalues extracts with key values used to create
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
use Text::CSV qw( csv );
use constant PROGNAME => "NVVOTER0 - -";

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
	'infile=s'     => \$voterFile,
	'outfile=s'    => \$voterValuesFile,
	'maxlines=s'   => \$maxLines,
	'maxfiles=n'   => \$maxFiles,
	'help!'        => \$helpReq,
	
=cut

my $records;

my $voterFile = "VoterList.ElgbVtr.45099.073019143713.csv";
#my $voterFile = "test1.elgbl.voter-5.csv";
my $voterFileh;
my @voterDataLine = ();
my %voterDataLine;

my $voterValuesFile = "votervalues.csv";
my $voterValuesFileh;
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
    "Precinct",          #1
    "LastName",          #2
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
        'infile=s'   => \$voterFile,
        'outfile=s'  => \$voterValuesFile,
        'maxlines=n' => \$maxLines,
        'maxfiles=n' => \$maxFiles,
        'help!'      => \$helpReq,

    ) or die "Incorrect usage! \n";
    if ($helpReq) {
        print "Come on, it's really not that hard. \n";
    }
    else {
        printLine("My inputfile is: $voterFile. \n");
    }
    unless ( open( $voterFileh, $voterFile ) ) {
        printLine("Unable to open INPUT: $voterFile Reason: $! \n");
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
    @csvHeadings = $csv->header($voterFileh);

    # on input these column headers contained a space - replace headers
    $csvHeadings[2]  = "firstname";
    $csvHeadings[3]  = "middlename";
    $csvHeadings[4]  = "lastname";
    $csvHeadings[6]  = "birthdate";
    $csvHeadings[7]  = "registrationdate";
    $csvHeadings[8]  = "address1";
    $csvHeadings[9]  = "address2";
    $csvHeadings[15] = "congressionaldistrict";
    $csvHeadings[16] = "senatedistrict";
    $csvHeadings[17] = "assemblydistrict";
    $csvHeadings[18] = "educationdistrict";
    $csvHeadings[19] = "regentdistrict";
    $csvHeadings[20] = "registeredprecinct";
    $csvHeadings[21] = "countystatus";
    $csvHeadings[22] = "countyvoterid";
    $csvHeadings[23] = "idrequired";
    $csv->column_names(@csvHeadings);

    # Build heading for new voting record
    $voterValuesHeading = join( ",", @voterValuesHeading );
    $voterValuesHeading = $voterValuesHeading . "\n";
    open( $voterValuesFileh, ">$voterValuesFile" )
      or die "Unable to open base: $voterValuesFile Reason: $! \n";
    print $voterValuesFileh $voterValuesHeading;

    #
    # Initialize process loop and open first output
    $linesRead    = 0;
    $linesIncRead = 0;

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
    while ( $line1Read = $csv->getline_hr($voterFileh) ) {
        $linesRead++;
        $linesIncRead++;
        if ( $linesIncRead == 1000 ) {
            printLine ("$linesRead lines processed \n");
            $linesIncRead = 0;
        }

        # then create the values array to complete preprocessing
        %csvRowHash = %{$line1Read};

        # - - - - - - - - - - - - - - - - - - - - - - - - -
        # Create hash of line for transformation
        # - - - - - - - - - - - - - - - - - - - - - - - - -
        # for first record of a series for a voter

        # votes by election

        $voterValuesLine{"state-voter-id"} = $csvRowHash{"voterid"};
        $voterValuesLine{"Party"}          = $csvRowHash{"party"};
        $voterValuesLine{"LastName"}       = $csvRowHash{"lastname"};
        $voterValuesLine{"Precinct"}       = $csvRowHash{"registeredprecinct"};
        my @date = split( /\s*\/\s*/, $csvRowHash{"birthdate"}, -1 );
        $mm = sprintf( "%02d", $date[0] );
        $dd = sprintf( "%02d", $date[1] );
        $yy = sprintf( "%02d", $date[2] );
        $voterValuesLine{"Birthdate"} = "$mm/$dd/$yy";
        @date = split( /\s*\/\s*/, $csvRowHash{"registrationdate"}, -1 );
        $mm   = sprintf( "%02d", $date[0] );
        $dd   = sprintf( "%02d", $date[1] );
        $yy   = sprintf( "%02d", $date[2] );
        $voterValuesLine{"Reg-Date"} = "$mm/$dd/$yy";
        $voterValuesLine{"Status"}   = $csvRowHash{"countystatus"};

        # prepare to write out the voter data, write it
        @voterValues = ();
        foreach (@voterValuesHeading) {
            push( @voterValues, $voterValuesLine{$_} );
        }
        print $voterValuesFileh join( ',', @voterValues ), "\n";
        %voterValuesLine = ();
        $linesWritten++;
        goto NEW;
    }

    #
    # For now this is the in-elegant way I detect completion
    if ( eof($voterFileh) ) {
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

close($voterFileh);
close($voterValuesFileh);

printLine("<===> Completed processing of: $voterFile \n");
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
    print (PROGNAME . $datestring . ' ' . $printData);
}
