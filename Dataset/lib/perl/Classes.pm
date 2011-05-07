package ReFlow::Dataset::Classes;

use strict;

use XML::Simple;
use Data::Dumper;

sub new {
  my ($class, $classesFile) = @_;

  my $self = {};

  bless($self,$class);

  my $self->{xml} = new XML::Simple();

  $self->_parseXmlFile($classesFile);

  return $self;
}

sub validateClassName {
  my ($self, $className) = @_;

  return $self->{data}->{datasetClass}->{$className};
}

# get list of plans used
sub getPlan2Classes {
    my ($self, $classNamesUsed) = @_;
    my %plan2classes;
    foreach my $className (@$classNamesUsed) {
      my $planFile = $self->{data}->{datasetClass}->{$className}->{graphFile}->{planFile};
      push(@{$plan2classes{$planFile}}, $className) if $planFile;
    }
    return \%plan2classes;
}

sub getClass {
    my ($self, $className) = @_;
    my $class = $self->{data}->{datasetClass}->{$className};
    die "Can't find class with name '$className'\n" unless $class;
    return $class;
}

sub getResourceText {
    my ($self, $dataset) = @_;
    my $class = $self->getClass($dataset->{class});
    my $resource = $class->{resource};
    return "" unless $resource;

    my $rawResourceText = $self->{xml}->xmlOut($resource);
    my ($resourceText, $err) = substitutePropsIntoXmlText($rawResourceText, $dataset);
    if ($err) {
	die "the <resource> element in class '$dataset->{class}' contains an invalid macro:\n$err\n";
    }
}

# static method
sub substitutePropsIntoXmlText {
    my ($xmlText, $dataset) = @_;

    my $newXmlText = $xmlText;
    foreach my $propKey (keys(%{$dataset->{prop}})) {
      my $propValue = $datasetAsHash->{prop}->{$propKey};
      $newXmlText =~ s/\$\{$propKey\}/$propValue/g;
    }

    my $err = 0;
    if ($xmlText =~ /(\$\{.*?\})/) {
	$err = $1;
    }
    return ($newXmlText, $err);
}

sub _parseXmlFile {
  my ($self, $classesFile) = @_;

  $self->{data} = eval{ $self->{xml}->XMLin($classesFile, SuppressEmpty => undef, KeyAttr=>'class', ForceArray=>['datasetClass']) };
#  print STDERR Dumper $self->{data};
  die "$@\nerror processing classes XML file $classesFile\n" if($@);
}

1;
