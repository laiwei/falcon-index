package doc

type Doc struct {
	ID      string
	MetaDoc *MetaDoc
}

func (m *Doc) Terms() []string {
	terms := []string{}
	if m != nil && m.MetaDoc.Endpoint != nil {
		terms = append(terms, "endpoint="+*m.MetaDoc.Endpoint)
	}
	if m != nil && m.MetaDoc.Metric != nil {
		terms = append(terms, "metric="+*m.MetaDoc.Metric)
	}
	if m != nil {
		for _, p := range m.MetaDoc.Tags {
			terms = append(terms, *p.Key+"="+*p.Value)
		}
	}
	return terms
}

func (m *Doc) TermDict() map[string]string {
	rt := make(map[string]string)
	if m != nil && m.MetaDoc.Endpoint != nil {
		rt["endpoint"] = *m.MetaDoc.Endpoint
	}
	if m != nil && m.MetaDoc.Metric != nil {
		rt["metric"] = *m.MetaDoc.Metric
	}
	if m != nil {
		for _, p := range m.MetaDoc.Tags {
			rt[*p.Key] = *p.Value
		}
	}
	return rt
}
