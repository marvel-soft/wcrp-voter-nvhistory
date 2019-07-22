#- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
# wcrp-voter-splitter
#
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
use Math::Round;

no warnings "uninitialized";

use constant {
 PERIOD20 => "11/06/2018",
 PERIOD19 => "06/12/2018",
 PERIOD18 => "11/08/2016",
 PERIOD17 => "06/14/2016",
 PERIOD16 => "11/04/2014",
 PERIOD15 => "06/10/2014",
 PERIOD14 => "11/06/2012",
 PERIOD13 => "06/12/2012",
 PERIOD12 => "09/13/2011",
 PERIOD11 => "11/02/2010",
 PERIOD10 => "06/08/2010",
 PERIOD09 => "11/04/2008",
 PERIOD08 => "08/12/2008",
 PERIOD07 => "11/07/2006",
 PERIOD06 => "08/05/2006",
 PERIOD05 => "11/02/2004",
 PERIOD04 => "09/07/2004",
 PERIOD03 => "06/03/2003",
 PERIOD02 => "11/05/2002",
 PERIOD01 => "09/03/2002",
};

=head1 Function
=over
=head2 Overview
	This program will split large csv files into smaller ones
		a) no restrictions
		b)
	Input: any csv file with headers
	       
	Output: one or more smaller csv file
	parms:
	'infile=s'     => \$inputFile,
	'outfile=s'    => \$outputFile,
	'maxlines=s'   => \$maxLines,
	'maxfiles=n'   => \$maxFiles,
	'help!'        => \$helpReq,
	
=cut

my $records;
my $inputFile = "nvsos-voter-history-test.csv";    

my $fileName         = "";

my $outputFile        = "extracts.csv";
my $outputFileh;

my $printFile        = "print-.txt";
my $printFileh;


my $helpReq            = 0;
my $fileCount          = 0;

my $csvHeadings        = "";
my @csvHeadings;
my $line1Read    = '';
my $linesRead    = 0;
my $printData;
my $linesWritten = 0;
my $maxFiles;
my $maxLines;
my $stateVoterId = 0;
my @values1;


my $voterStatHeading = "";
my @voterStatHeading = (
	"state-voter-id",         #0
	"Voter Status",			#1
	"Precinct",    			#2    
	"Last Name",			  #3
	"null1",          	#4
	"null2",	        	#5
	"null3",				    #6
	"Generals",     		#7
	"Primaries",			  #8
	"Polls",          	#9
	"Absentee",				  #10
	"LeansDEM",      		#11
	"LeansREP",      		#12
	"Leans",				    #13
	"Rank",					    #14
  "gender",	        	#15
	"military",				  #16

);

my @csvRowHash;
my %csvRowHash = ();

my $voterDataHeading = "";
my @voterDataHeading = (
  "state-voter-id",         
  PERIOD20,
  PERIOD19,
  PERIOD18,
  PERIOD17,
  PERIOD16,
  PERIOD15,
  PERIOD14,
  PERIOD13,
  PERIOD12,
  PERIOD11,
  PERIOD10,
  PERIOD09,
  PERIOD08,
  PERIOD07,
  PERIOD06,
  PERIOD05,
  PERIOD04,
  PERIOD03,
  PERIOD02,
  PERIOD01,
);


my $baseFile         = "extract.csv";
my $baseFileh;
my %baseLine         = ();

#
# main program controller
#
sub main {
	#Open file for messages and errors
	open( $printFileh, ">$printFile" )
	  or die "Unable to open PRINT: $printFile Reason: $!";

	# Parse any parameters
	GetOptions(
		'infile=s'     => \$inputFile,
		'outfile=s'    => \$outputFile,
		'maxlines=n'   => \$maxLines,
		'maxfiles=n'   => \$maxFiles,
		'help!'        => \$helpReq,

	) or die "Incorrect usage! \n";
	if ($helpReq) {
		print "Come on, it's really not that hard. \n";
	}
	else {
		printLine ("My inputfile is: $inputFile. \n");
	}
	unless ( open( INPUT, $inputFile ) ) {
		printLine ("Unable to open INPUT: $inputFile Reason: $! \n");
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

	open( $outputFileh, ">$outputFile" )
	  or die "Unable to open output: $outputFile Reason: $! \n";
  print $outputFileh $voterDataHeading;

	#
	# Initialize process loop and open first output
  $linesRead = 0;

  NEW:
	while ( $line1Read = <INPUT> ) {
	
		$linesRead++;
	
		$linesIncRead++;
		if ($linesIncRead == 10000) {
			printLine ("$linesRead lines processed \n");
			$linesIncRead = 0; 
		}
		
		# replace commas from in between double quotes with a space
		chomp $line1Read;
	  chop $line1Read;
		$line1Read =~ s/(?:\G(?!\A)|[^"]*")[^",]*\K(?:,|"(*SKIP)(*FAIL))/ /g;

				# then create the values array
		@values1 = split( /\s*,\s*/, $line1Read, -1 );

		# Create hash of line for transformation
		@csvRowHash{@csvHeadings} = @values1;

		#- - - - - - - - - - - - - - - - - - - - - - - - - - 
		# Assemble database load  for base segment
		#- - - - - - - - - - - - - - - - - - - - - - - - - - 
		%baseLine = ();
		$baseLine{"state-voter-id"}     = $csvRowHash{"voter-id"};


	  print $outputFileh $line1Read. "\n";
		
		$linesWritten++;
		#
		# For now this is the in-elegant way I detect completion
	
		if ( eof(INPUT) ) {
			goto EXIT;
		}
		next;
		}
	#
	goto NEW;
	}	

#
# call main program controller
main();
#
# Common Exit
EXIT:

printLine ("<===> Completed processing of: $inputFile \n");
printLine ("<===> Total Records Read: $linesRead \n");
printLine ("<===> Total Records written: $linesWritten \n");

close(INPUT);
close($outputFileh);
close($printFileh);
exit;

#
# Print report line
#
sub printLine  {
	my $datestring = localtime();
	($printData) = @_;
	print $printFileh $datestring . ' ' . $printData;
	print $datestring . ' ' . $printData;
}
