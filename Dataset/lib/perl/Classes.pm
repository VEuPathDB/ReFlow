package ReFlow::Dataset::Classes;

use strict;

use XML::Simple;
use Data::Dumper;

sub new {
  my ($class, $classesFile) = @_;

  my $self = {};

  bless($self,$class);

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

sub _parseXmlFile {
  my ($self, $classesFile) = @_;

  my $xml = new XML::Simple();
  $self->{data} = eval{ $xml->XMLin($classesFile, SuppressEmpty => undef, KeyAttr=>'class', ForceArray=>['datasetClass']) };
#  print STDERR Dumper $self->{data};
  die "$@\nerror processing classes XML file $classesFile\n" if($@);
}

1;
