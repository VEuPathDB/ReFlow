package ReFlow::Dataset::Datasets;

use strict;

use File::Basename;

use XML::Simple;
use Data::Dumper;

sub new {
  my ($class, $datasetsFile, $classes) = @_;

  my $self = {
      classes => $classes,
      datasetsFile => $datasetsFile
  };

  bless($self,$class);

  $self->_parseXmlFile($datasetsFile);

  my ($name,$path,$suffix) = fileparse($datasetsFile, '.xml');

  $path =~ /.*lib\/xml\/datasets\/(.*)/ || die "Dataset file name '$datasetsFile' is not in a path including lib/xml/datasets";
  $self->{fullName} = "$1$name";

  return $self;
}

sub validateAgainstClasses {
  my ($self) = @_;
  
  my $datasets = $self->{data}->{dataset};
  my $errs = {};
  foreach my $dataset (@$datasets) {
      my $className = $dataset->{class};
      my $class = $self->{classes}->getClass($className);
      my $classProps = $class->getProps();
      
      foreach my $prop (@$classProps) {
	  $self->datasetErr($dataset, $prop) unless $dataset->{prop}->{$prop};
      }
  }
}

# have to print out whole dataset xml because there is no identifier for
# a dataset
sub datasetErr {
  my ($self, $dataset, $missingProp) = @_;
  my @props;
  foreach my $prop (keys(%{$dataset->{prop}})) {
      push (@props, "    <prop name=\"$prop\">$dataset->{prop}->{$prop}</prop.");
  }

  my $propsStr = join("\n",@props);
  my $classesFile = $self->{classes}->getClassFile();
  my $className = $dataset->{class};

  die "
Error.  The following dataset in file $self->{datasetsFile} is missing <prop name=\"$missingProp\">.  Compare it with the dataset class in file $classesFile
  <dataset class=\"$className\">$propsStr
  </dataset>\n\n"
}

sub getClassNamesUsed {
  my ($self) = @_;
  if (!$self->{classNamesUsed}) {
    my $datasets = $self->{data}->{dataset};
    $self->{classNamesUsed} = {};
    foreach my $dataset (@$datasets) {
      my $class = $dataset->{class};
      $self->{classNamesUsed}->{$class} = [] unless $self->{classNamesUsed}->{$class};
      push(@{$self->{classNamesUsed}->{$class}}, $dataset);
    }
  }
  my @a = keys(%{$self->{classNamesUsed}});
  return \@a;
}

sub getFullName {
  my ($self) = @_;
  return $self->{fullName};
}

sub getDatasets {
  my ($self) = @_;
  return $self->{data}->{dataset};
}

sub getDatasetsByClass {
    my ($self, $className) = @_;
    $self->getClassNamesUsed();  # in case it hasn't been called yet
    return $self->{classNamesUsed}->{$className};

}

sub printResourceXml {
    my ($self) = @_;
    my $xml = new XML::Simple();
    print STDOUT $xml->xmlOut($self->{data});
}

sub _parseXmlFile {
  my ($self, $datasetsFile) = @_;

  my $xml = new XML::Simple();
  my $firstParse = eval{ $xml->XMLin($datasetsFile, SuppressEmpty => undef, ForceArray=>['constant'])};

  my $constants = $firstParse->{constant};

  my $xmlString = $self->_substituteConstants($datasetsFile, $constants);

  # parse again, this time w/ constants resolved
  $self->{data} = eval{ $xml->XMLin($xmlString, SuppressEmpty => undef, ForceArray=>['dataset', 'prop'], KeyAttr=>{prop=>'name'}) };

#  print STDOUT Dumper $self->{data};
  die "$@\nerror processing XML file $datasetsFile\n" if($@);
}

sub _substituteConstants {
  my ($self, $xmlFile, $constants) = @_;

  my $xmlString;
  open(FILE, $xmlFile) || die "Cannot open datasets XML file '$xmlFile'\n";
  while (<FILE>) {
    my $line = $_;
    my @constantKeys = /\$\$([\w.]+)\$\$/g;   # allow keys of the form nrdb.release
    foreach my $constantKey (@constantKeys) {
      my $val = $constants->{$constantKey}->{value};
      die "Invalid constant '\$\$$constantKey\$\$' in xml file $xmlFile\n" unless defined $val;
      $line =~ s/\$\$$constantKey\$\$/$val/g;
    }
    $xmlString .= $line;
  }
  return $xmlString;
}


1;
