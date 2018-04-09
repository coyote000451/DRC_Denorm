#! c:\perl\bin

use strict;
use warnings;
use diagnostics;
use DBI;


# Make sure to run this an output file otherwise you won't have results.

# Variables

my $table 		= 	"drc_denorm ";
my $view		=	"V_vrx_drc_denorm";
my $SQLSRV		= 	"IPMULALLPRDDB01";
my $SQLDB		= 	"Global_Vantage";
my $LIMIT		=	"80000";
my $HOME		=	"c:\\temp\\drc_denorm";
my $VERSION		=	"224.00000";
my $COUNTRY_ID	=	"13";

# Build distinct ndc_code array
# Connect to the database

	my $dbh;
	my $DSN;
	#$DSN = 'driver={SQL Server};Server=MULQADB02; database=SDK_Distribute;TrustedConnection=Yes'; 
	#$DSN = 'driver={SQL Server};Server=\$SQLSRV; database=\$SQLDB;TrustedConnection=Yes'; 
	
	$DSN = 'driver={SQL Server};Server=IPMULALLPRDDB01; database=Global_Vantage;TrustedConnection=Yes'; 
	$dbh = DBI->connect("dbi:ODBC:$DSN") or die "$DBI::errstr\n";
	$dbh->{'LongTruncOk'} = 1;
	$dbh->{'LongReadLen'} = 65535;

# Build the ndc_code array

	my $sth = $dbh->prepare("SELECT distinct top $LIMIT main_multum_drug_code FROM $table order by main_multum_drug_code");
	$sth->execute;	
	
	my @main_multum_drug_code_distinct;
	
	while( my $row = $sth->fetchrow_array())
	{
			push @main_multum_drug_code_distinct, $row;
	}


my $main_multum_drug_code_size = @main_multum_drug_code_distinct;

print "main_multum_drug_code_size:  $main_multum_drug_code_size\n";

my @ColArray = qw(main_multum_drug_code type_description age_description weight_description condition1_description condition2_description route_description 
liver_description renal_description gender_description smoker_description amount_low amount_high unit_dose_abbreviation amount_time unit_time_abbreviation 
max_frequency comment_description lifetime_comment prn_flag chronic_therapy_flag case_id);

	for my $COL (@ColArray)
	{
	
			# Open the file based on the column name and write to disk
			open FILE, ">", "c:\\temp\\drc_denorm\\sqlsrv$COL.bat" or die $!;
		
			print FILE "\"C:\\Program Files\\Microsoft SQL Server\\110\\Tools\\Binn\\sqlcmd\"";	
			
			if ($COL =~ m/main_multum_drug_code/)
			{
				print FILE " -S $SQLSRV -d $SQLDB -E -W -h -1 -s \"|\" -Q \"set nocount on; select top $LIMIT $COL from $table order by main_multum_drug_code \" -o \"$HOME\\sqlsrv_$COL.txt\"";
			}
			
			elsif (($COL =~ m/amount_low/) || ($COL =~ m/amount_high/) || ($COL =~ m/amount_time/))
			{
				print FILE " -S $SQLSRV -d $SQLDB -E -W -h -1 -s \"|\" -Q \"set nocount on; select top $LIMIT $COL from $table order by main_multum_drug_code, $COL \" -o \"$HOME\\sqlsrv_$COL.txt\"";
			}
			
			else
			{
				print FILE " -S $SQLSRV -d $SQLDB -E -W -h -1 -s \"|\" -Q \"set nocount on; select top $LIMIT ISNULL($COL, 'NULL') from $table order by main_multum_drug_code, $COL \" -o \"$HOME\\sqlsrv_$COL.txt\"";
			}
			close(FILE);
			system ("$HOME\\sqlsrv$COL.bat");

	}

	for my $COL (@ColArray)
	{

#	
		# Open the file based on the column name and write to disk
			open FILE, ">", "c:\\temp\\drc_denorm\\sqlite$COL.bat" or die $!;
			if ($COL =~ m/ndc_code/)
			{
				print FILE "sqlite3.exe en-US_VantageRx.odb \"select $COL from $table order by main_multum_drug_code LIMIT $LIMIT \" > $HOME\\sqlite_$COL.txt";
			}
			
			else
			{
				print FILE "sqlite3.exe en-US_VantageRx.odb \"select  IFNULL($COL, 'NULL') from $table order by main_multum_drug_code, $COL LIMIT $LIMIT \" > $HOME\\sqlite_$COL.txt";
			}
		
			close(FILE);
			system ("$HOME\\sqlite$COL.bat");
#		}
#		
	}

use ReadFile;
use File::Copy;


for my $COL (@ColArray)
{
	my @FileArray 		= ReadFile->new("$HOME\\sqlsrv_$COL.txt")->GetFile();
	
	if (($COL =~ m/unit_dose_abbreviation/) || ($COL =~ m/comment_description/))
	{
		my @SortFileArray 	= sort @FileArray;
		@FileArray			= @SortFileArray;
	}
	
# SQLite

	my @SQLiteFileArray 	= ReadFile->new("$HOME\\sqlite_$COL.txt")->GetFile();

	if (($COL =~ m/unit_dose_abbreviation/) || ($COL =~ m/comment_description/))
	{
		my @SortSQLiteFileArray = sort @SQLiteFileArray;
		@SQLiteFileArray		= @SortSQLiteFileArray;
	}

	print "\n";
	my $SQLSRVSize 			= 	@FileArray;
	my $SQLiteFileSize		=	@SQLiteFileArray;
	
	print "SQL Server Array size $SQLSRVSize\n";
	print "SQLite Array size $SQLiteFileSize\n";
	
for (my $i = 0; $i < $SQLiteFileSize; $i++)
{

	$FileArray[$i] 			=~ s/\(//g; # remove "("
	$FileArray[$i] 			=~ s/\)//g; # remove ")"
	$SQLiteFileArray[$i]	=~ s/\(//g; # remove "("
	$SQLiteFileArray[$i]	=~ s/\)//g; # remove ")"
	
	$FileArray[$i] 			=~ s/\*//g; # remove "*"
	$SQLiteFileArray[$i]	=~ s/\*//g; # remove "*"
	
	
	if ($COL =~ m/amount_low/)
	{

			$SQLiteFileArray[$i] =~ s/^0+//; # remove the leading zero
	
	}
	
	if ($COL =~ m/amount_high/)
	{

			$SQLiteFileArray[$i] =~ s/^0+//; # remove the leading zero

	}

	
	if ($FileArray[$i] =~ m/$SQLiteFileArray[$i]/)
	{
		#print "$FileArray[$i] MATCHS $SQLiteFileArray[$i] at INDEX $i at $COL\n";
	}
	else 
	{
		print "$FileArray[$i] NOMATCH $SQLiteFileArray[$i] at INDEX $i at $COL\n";
	}
	
}


}
