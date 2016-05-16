package ReFlow::DatasetClass::Dataset2PropsFile;

use strict;
use lib "$ENV{GUS_HOME}/lib/perl";
use XML::Twig;
use File::Basename;
use ReFlow::DatasetClass::Datasets;
use ReFlow::DatasetClass::Classes;
use ReFlow::DatasetClass::Template;
use File::Path;
use File::Copy;

##########################  static methods to support dataset2propsfile commands  ##################################

sub initializeForDatasets2PropsFile {
  my ($class, $classesFile, $datasetClassCategoriesFile, $categoryNamesFile, $doNotValidateXml) = @_;

  if (!$doNotValidateXml) {
    system("validateXmlWithRng  $ENV{GUS_HOME}/lib/rng/datasetClass.rng $classesFile") && exit(1);
  }

  # parse class file
  my $classes = ReFlow::DatasetClass::Classes->new($classesFile);

  # parse categoryNamesFile
  open(ANNOT, $categoryNamesFile) or die "Cannot open categoryNames file $categoryNamesFile for reading:$!";
  my %categoryNames;
  while (<ANNOT>) {
    chomp;
    my @annot = split(/\t/, $_);
    next unless($annot[1]);
    $categoryNames{$annot[1]} = $annot[2];
  }
  close ANNOT;

  # parse datasetCategories file
  open(FILE, $datasetClassCategoriesFile) or die "Cannot open datasetClassCategories file $datasetClassCategoriesFile for reading:$!";
  my %datasetClassCategories;
  while (<FILE>) {
    chomp;
    my @dcc = split(/\t/, $_);
    $datasetClassCategories{$dcc[0]}->{categoryDisplayName} = $categoryNames{$dcc[1]} ? $categoryNames{$dcc[1]} : $dcc[1];
    $datasetClassCategories{$dcc[0]}->{categoryIri} = $dcc[1];
  }
  close FILE;

  return ($classes, \%datasetClassCategories);
}

sub dataset2PropsFile {
  my ($class, $datasetFile, $classes, $datasetClassCategories) = @_;

  # parse datasets file
  my $datasets = ReFlow::DatasetClass::Datasets->new($datasetFile, $classes);
  my $classNamesUsed = $datasets->getClassNamesUsed();

  # validate all dataset properties against classes
  $datasets->validateAgainstClasses();

  my $datasetsFullName = $datasets->getFullName();
  my $datasetPropertiesFileName = "$ENV{GUS_HOME}/lib/prop/datasetProperties/${datasetsFullName}.prop";

  if (-e $datasetPropertiesFileName) {
    if (-M $datasetPropertiesFileName < -M $datasetFile) {
      print STDERR "File $datasetPropertiesFileName is up to date.\n";
      return;
    } else {
      unlink($datasetPropertiesFileName) || die "Couldn't delete old $datasetPropertiesFileName\n";
    }
  }

  my ($basename,$path,$suffix) = fileparse($datasetPropertiesFileName, '.prop');
  mkpath($path);
  open(F, ">$datasetPropertiesFileName.tmp") || die "Can't open file file '$datasetPropertiesFileName.tmp' for writing\n";

  foreach my $dataset (@{$datasets->getDatasets()}) {
    my $datasetPropertiesText = $classes->getDatasetPropertiesText($dataset, $datasetClassCategories);
    print F "$datasetPropertiesText\n\n";
  }
  close(F);

  move("$datasetPropertiesFileName.tmp", $datasetPropertiesFileName) || die "Couldn't move $datasetPropertiesFileName.tmp to $datasetPropertiesFileName\n";
}

1;
