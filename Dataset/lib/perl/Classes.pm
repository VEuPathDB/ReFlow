package ReFlow::Dataset::Classes;

use strict;

use XML::Simple;
use Data::Dumper;

sub new {
  my ($class, $classesFile) = @_;

  my $self = {};

  bless($self,$class);

  $self->_parseXmlFile($self->{classesFile});

  return $self;
}

sub getPlans2Classes {

}

# get list of plans used
sub getPlanFiles {
    my ($classNamesUsed) = @_;

}

sub _parseXmlFile {
  my ($self, $classesFile) = @_;

  my $xml = new XML::Simple();
  $self->{data} = eval{ $xml->XMLin($classesFile, SuppressEmpty => undef, KeyAttr => 'resource', ForceArray=>['publication','unpack', 'getAndUnpackOutput', 'resource', 'wdkReference']) };
#  print STDERR Dumper $self->{data};
  die "$@\nerror processing XML file $classesFile\n" if($@);
}

1;
