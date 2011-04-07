package ReFlow::DataSource::DataSourceInfo;

use strict;
use Data::Dumper;

sub new {
  my ($class, $dataSourceName, $parsedXml, $dataSources) = @_;

  my $self = {};
  $self->{parsedXml} = $parsedXml;
  $self->{dataSourceName} = $dataSourceName;
  $self->{dataSources} = $dataSources;

  bless($self,$class);

  return $self;
}

sub getName {
    my ($self) = @_;

    return $self->{dataSourceName};
}

sub getDisplayName {
    my ($self) = @_;

    return $self->{parsedXml}->{displayName};
}

sub getProject {
    my ($self) = @_;

    return $self->{parsedXml}->{project};
}

sub getCategory {
    my ($self) = @_;

    return $self->{parsedXml}->{category};
}

sub getContact {
    my ($self) = @_;

    return $self->{parsedXml}->{contact};
}

sub getEmail {
    my ($self) = @_;

    return $self->{parsedXml}->{email};
}

sub getInstitution {
    my ($self) = @_;

    return $self->{parsedXml}->{institution};
}

sub getPublicUrl {
    my ($self) = @_;

    return $self->{parsedXml}->{publicUrl};
}

sub getDescription {
    my ($self) = @_;

    return $self->{parsedXml}->{description};
}

# returns reference to an array of hash references with keys:
#   pmid, doi, citation
sub getPublications {
    my ($self) = @_;

    if (!$self->{publications}) {
	foreach my $publication (@{$self->{parsedXml}->{publication}}) {
	    my $pubmedId = $publication->{pmid};
	    if ($pubmedId) {
		$publication->{citation} = `pubmedIdToCitation $pubmedId`;
		die "failed calling 'pubmedIdToCitation $pubmedId'" if $? >> 8;
	    }
	}
	$self->{publications} = $self->{parsedXml}->{publication};
    }
    return $self->{publications};
}

# returns reference to an array of hash references with keys:
# recordClass, type, name
sub getWdkReferences {
    my ($self) = @_;

    return $self->{parsedXml}->{wdkReference};
}


1;
