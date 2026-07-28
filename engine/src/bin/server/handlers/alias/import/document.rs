//! Bounded decoding for the Elasticsearch-compatible synonym-rule envelope.

use serde::{
    de::{self, IgnoredAny, MapAccess, SeqAccess, Visitor},
    Deserialize, Deserializer,
};

use reverse_rusty::vocab::MAX_ALIAS_IMPORT_RULES;

pub(super) enum SynonymsSet {
    One(SynonymRule),
    Many(Vec<SynonymRule>),
}

impl<'de> Deserialize<'de> for SynonymsSet {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct SynonymsSetVisitor;

        impl<'de> Visitor<'de> for SynonymsSetVisitor {
            type Value = SynonymsSet;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("one synonym rule object or an array of rule objects")
            }

            fn visit_map<M>(self, map: M) -> Result<Self::Value, M::Error>
            where
                M: MapAccess<'de>,
            {
                SynonymRule::deserialize(de::value::MapAccessDeserializer::new(map))
                    .map(SynonymsSet::One)
            }

            fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                if sequence
                    .size_hint()
                    .is_some_and(|size| size > MAX_ALIAS_IMPORT_RULES)
                {
                    return Err(rule_limit_error());
                }

                let mut rules = Vec::with_capacity(
                    sequence
                        .size_hint()
                        .unwrap_or(0)
                        .min(MAX_ALIAS_IMPORT_RULES),
                );
                while rules.len() < MAX_ALIAS_IMPORT_RULES {
                    let Some(rule) = sequence.next_element()? else {
                        return Ok(SynonymsSet::Many(rules));
                    };
                    rules.push(rule);
                }
                if sequence.next_element::<IgnoredAny>()?.is_some() {
                    return Err(rule_limit_error());
                }
                Ok(SynonymsSet::Many(rules))
            }
        }

        deserializer.deserialize_any(SynonymsSetVisitor)
    }
}

fn rule_limit_error<E: de::Error>() -> E {
    E::custom(format!(
        "`synonyms_set` accepts at most {MAX_ALIAS_IMPORT_RULES} rule objects"
    ))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SynonymRule {
    /// Accepted for Elasticsearch request familiarity. Reverse Rusty's
    /// governed registry keys canonical form groups rather than rule IDs.
    #[serde(default)]
    pub(super) id: Option<String>,
    pub(super) synonyms: String,
}
