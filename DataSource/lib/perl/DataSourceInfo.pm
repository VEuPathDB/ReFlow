package ReFlow::DataSource::DataSourceInfo;

use strict;
use Data::Dumper;

sub new {
  my ($class, $resourceName, $parsedXml, $dataSourceInfos) = @_;

  my $self = {resourceName => $resourceName,
              displayName => $parsedXml->{displayName},
              project => $parsedXml->{project},
              category => $parsedXml->{category},
              contact => $parsedXml->{contact},
              email => $parsedXml->{email},
              institution => $parsedXml->{institution},
              publicUrl => $parsedXml->{publicUrl},
              description => $parsedXml->{description},
              wdkReference => $parsedXml->{wdkReference},
              parsedXml => $parsedXml,
              dataSourceInfos => $dataSourceInfos,
             };

  bless($self,$class);

  return $self;
}


sub getDataSourceInfos {
  my ($self) = @_;

  return $self->{dataSourceInfos};
}

sub getParsedXml {
  my ($self) = @_;

  return $self->{parsedXml};
}

sub getName {
    my ($self) = @_;

    return $self->{dataSourceName};
}

sub getDisplayName {
    my ($self) = @_;

    return $self->{displayName};
}

sub getProject {
    my ($self) = @_;

    return $self->{project};
}

sub getCategory {
    my ($self) = @_;

    return $self->{category};
}

sub getContact {
    my ($self) = @_;

    return $self->{contact};
}

sub getEmail {
    my ($self) = @_;

    return $self->{email};
}

sub getInstitution {
    my ($self) = @_;

    return $self->{institution};
}

sub getPublicUrl {
    my ($self) = @_;

    return $self->{publicUrl};
}

sub getDescription {
    my ($self) = @_;

    return $self->{description};
}

# returns reference to an array of hash references with keys:
#   pmid, doi, citation
sub getPublications {
    my ($self) = @_;

    my $parsedXml = $self->getParsedXml();

    if (!$self->{publications}) {

	my $publications = ref($parsedXml->{publication}) eq 'ARRAY' ? $parsedXml->{publication} : [];

	foreach my $publication (@$publications) {
	    my $pubmedId = $publication->{pmid};
	    if ($pubmedId) {
		$publication->{citation} = `pubmedIdToCitation $pubmedId`;
		die "failed calling 'pubmedIdToCitation $pubmedId'" if $? >> 8;
	    }
	}
	$self->{publications} = $publications;
    }
    return $self->{publications};
}

# returns reference to an array of hash references with keys:
# recordClass, type, name
sub getWdkReferences {
    my ($self) = @_;

    return $self->{wdkReference};
}


1;
