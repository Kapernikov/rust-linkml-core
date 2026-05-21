#![allow(non_camel_case_types)]

use crate::poly_containers::*;
use crate::*;

pub trait AnyValue {}

impl AnyValue for crate::AnyValue {}

pub trait Extension {
    fn extension_tag<'a>(&'a self) -> &'a crate::uriorcurie;
    // fn extension_tag_mut(&mut self) -> &mut &'a crate::uriorcurie;
    // fn set_extension_tag(&mut self, value: uriorcurie);

    fn extension_value<'a>(&'a self) -> &'a crate::AnyValue;
    // fn extension_value_mut(&mut self) -> &mut &'a crate::AnyValue;
    // fn set_extension_value<E>(&mut self, value: E) where E: Into<AnyValue>;

    fn extensions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, ExtensionOrSubtype>>;
    // fn extensions_mut(&mut self) -> &mut Option<impl poly_containers::MapRef<'a, String,ExtensionOrSubtype>>;
    // fn set_extensions<E>(&mut self, value: Option<&HashMap<String, E>>) where E: Into<Extension>;
}

impl Extension for crate::Extension {
    fn extension_tag<'a>(&'a self) -> &'a crate::uriorcurie {
        return &self.extension_tag;
    }
    fn extension_value<'a>(&'a self) -> &'a crate::AnyValue {
        return &self.extension_value;
    }
    fn extensions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, ExtensionOrSubtype>> {
        return self.extensions.as_ref().map(poly_containers::MapView::new);
    }
}
impl Extension for crate::Annotation {
    fn extension_tag<'a>(&'a self) -> &'a crate::uriorcurie {
        return &self.extension_tag;
    }
    fn extension_value<'a>(&'a self) -> &'a crate::AnyValue {
        return &self.extension_value;
    }
    fn extensions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, ExtensionOrSubtype>> {
        return self.extensions.as_ref();
    }
}

impl Extension for crate::ExtensionOrSubtype {
    fn extension_tag<'a>(&'a self) -> &'a crate::uriorcurie {
        match self {
            ExtensionOrSubtype::Annotation(val) => val.extension_tag(),
        }
    }
    fn extension_value<'a>(&'a self) -> &'a crate::AnyValue {
        match self {
            ExtensionOrSubtype::Annotation(val) => val.extension_value(),
        }
    }
    fn extensions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, ExtensionOrSubtype>> {
        match self {
            ExtensionOrSubtype::Annotation(val) => val.extensions().map(|x| x.to_any()),
        }
    }
}

pub trait Extensible {
    fn extensions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, ExtensionOrSubtype>>;
    // fn extensions_mut(&mut self) -> &mut Option<impl poly_containers::MapRef<'a, String,ExtensionOrSubtype>>;
    // fn set_extensions<E>(&mut self, value: Option<&HashMap<String, E>>) where E: Into<Extension>;
}

impl Extensible for crate::Extensible {
    fn extensions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, ExtensionOrSubtype>> {
        return self.extensions.as_ref();
    }
}
impl Extensible for crate::Element {
    fn extensions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, ExtensionOrSubtype>> {
        return self.extensions.as_ref();
    }
}
impl Extensible for crate::EnumBinding {
    fn extensions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, ExtensionOrSubtype>> {
        return self.extensions.as_ref();
    }
}
impl Extensible for crate::StructuredAlias {
    fn extensions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, ExtensionOrSubtype>> {
        return self.extensions.as_ref();
    }
}
impl Extensible for crate::AnonymousExpression {
    fn extensions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, ExtensionOrSubtype>> {
        return self.extensions.as_ref();
    }
}
impl Extensible for crate::PathExpression {
    fn extensions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, ExtensionOrSubtype>> {
        return self.extensions.as_ref();
    }
}
impl Extensible for crate::ClassRule {
    fn extensions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, ExtensionOrSubtype>> {
        return self.extensions.as_ref();
    }
}
impl Extensible for crate::ArrayExpression {
    fn extensions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, ExtensionOrSubtype>> {
        return self.extensions.as_ref();
    }
}
impl Extensible for crate::DimensionExpression {
    fn extensions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, ExtensionOrSubtype>> {
        return self.extensions.as_ref();
    }
}
impl Extensible for crate::PatternExpression {
    fn extensions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, ExtensionOrSubtype>> {
        return self.extensions.as_ref();
    }
}
impl Extensible for crate::ImportExpression {
    fn extensions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, ExtensionOrSubtype>> {
        return self.extensions.as_ref();
    }
}
impl Extensible for crate::PermissibleValue {
    fn extensions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, ExtensionOrSubtype>> {
        return self.extensions.as_ref();
    }
}
impl Extensible for crate::UniqueKey {
    fn extensions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, ExtensionOrSubtype>> {
        return self.extensions.as_ref();
    }
}
impl Extensible for crate::TypeMapping {
    fn extensions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, ExtensionOrSubtype>> {
        return self.extensions.as_ref();
    }
}
impl Extensible for crate::AnonymousSlotExpression {
    fn extensions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, ExtensionOrSubtype>> {
        return self.extensions.as_ref();
    }
}
impl Extensible for crate::AnonymousClassExpression {
    fn extensions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, ExtensionOrSubtype>> {
        return self.extensions.as_ref();
    }
}
impl Extensible for crate::SchemaDefinition {
    fn extensions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, ExtensionOrSubtype>> {
        return self.extensions.as_ref();
    }
}
impl Extensible for crate::TypeDefinition {
    fn extensions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, ExtensionOrSubtype>> {
        return self.extensions.as_ref();
    }
}
impl Extensible for crate::SubsetDefinition {
    fn extensions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, ExtensionOrSubtype>> {
        return self.extensions.as_ref();
    }
}
impl Extensible for crate::Definition {
    fn extensions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, ExtensionOrSubtype>> {
        return self.extensions.as_ref();
    }
}
impl Extensible for crate::EnumDefinition {
    fn extensions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, ExtensionOrSubtype>> {
        return self.extensions.as_ref();
    }
}
impl Extensible for crate::SlotDefinition {
    fn extensions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, ExtensionOrSubtype>> {
        return self.extensions.as_ref();
    }
}
impl Extensible for crate::ClassDefinition {
    fn extensions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, ExtensionOrSubtype>> {
        return self.extensions.as_ref();
    }
}

impl Extensible for crate::ExtensibleOrSubtype {
    fn extensions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, ExtensionOrSubtype>> {
        match self {
            ExtensibleOrSubtype::Element(val) => val.extensions().map(|x| x.to_any()),
            ExtensibleOrSubtype::EnumBinding(val) => val.extensions().map(|x| x.to_any()),
            ExtensibleOrSubtype::StructuredAlias(val) => val.extensions().map(|x| x.to_any()),
            ExtensibleOrSubtype::AnonymousExpression(val) => val.extensions().map(|x| x.to_any()),
            ExtensibleOrSubtype::PathExpression(val) => val.extensions().map(|x| x.to_any()),
            ExtensibleOrSubtype::ClassRule(val) => val.extensions().map(|x| x.to_any()),
            ExtensibleOrSubtype::ArrayExpression(val) => val.extensions().map(|x| x.to_any()),
            ExtensibleOrSubtype::DimensionExpression(val) => val.extensions().map(|x| x.to_any()),
            ExtensibleOrSubtype::PatternExpression(val) => val.extensions().map(|x| x.to_any()),
            ExtensibleOrSubtype::ImportExpression(val) => val.extensions().map(|x| x.to_any()),
            ExtensibleOrSubtype::PermissibleValue(val) => val.extensions().map(|x| x.to_any()),
            ExtensibleOrSubtype::UniqueKey(val) => val.extensions().map(|x| x.to_any()),
            ExtensibleOrSubtype::TypeMapping(val) => val.extensions().map(|x| x.to_any()),
            ExtensibleOrSubtype::AnonymousSlotExpression(val) => {
                val.extensions().map(|x| x.to_any())
            }
            ExtensibleOrSubtype::AnonymousClassExpression(val) => {
                val.extensions().map(|x| x.to_any())
            }
            ExtensibleOrSubtype::SchemaDefinition(val) => val.extensions().map(|x| x.to_any()),
            ExtensibleOrSubtype::TypeDefinition(val) => val.extensions().map(|x| x.to_any()),
            ExtensibleOrSubtype::SubsetDefinition(val) => val.extensions().map(|x| x.to_any()),
            ExtensibleOrSubtype::Definition(val) => val.extensions().map(|x| x.to_any()),
            ExtensibleOrSubtype::EnumDefinition(val) => val.extensions().map(|x| x.to_any()),
            ExtensibleOrSubtype::SlotDefinition(val) => val.extensions().map(|x| x.to_any()),
            ExtensibleOrSubtype::ClassDefinition(val) => val.extensions().map(|x| x.to_any()),
        }
    }
}
impl Extensible for crate::ElementOrSubtype {
    fn extensions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, ExtensionOrSubtype>> {
        match self {
            ElementOrSubtype::SchemaDefinition(val) => val.extensions().map(|x| x.to_any()),
            ElementOrSubtype::TypeDefinition(val) => val.extensions().map(|x| x.to_any()),
            ElementOrSubtype::SubsetDefinition(val) => val.extensions().map(|x| x.to_any()),
            ElementOrSubtype::Definition(val) => val.extensions().map(|x| x.to_any()),
            ElementOrSubtype::EnumDefinition(val) => val.extensions().map(|x| x.to_any()),
            ElementOrSubtype::SlotDefinition(val) => val.extensions().map(|x| x.to_any()),
            ElementOrSubtype::ClassDefinition(val) => val.extensions().map(|x| x.to_any()),
        }
    }
}
impl Extensible for crate::AnonymousExpressionOrSubtype {
    fn extensions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, ExtensionOrSubtype>> {
        match self {
            AnonymousExpressionOrSubtype::AnonymousSlotExpression(val) => {
                val.extensions().map(|x| x.to_any())
            }
            AnonymousExpressionOrSubtype::AnonymousClassExpression(val) => {
                val.extensions().map(|x| x.to_any())
            }
        }
    }
}
impl Extensible for crate::DefinitionOrSubtype {
    fn extensions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, ExtensionOrSubtype>> {
        match self {
            DefinitionOrSubtype::EnumDefinition(val) => val.extensions().map(|x| x.to_any()),
            DefinitionOrSubtype::SlotDefinition(val) => val.extensions().map(|x| x.to_any()),
            DefinitionOrSubtype::ClassDefinition(val) => val.extensions().map(|x| x.to_any()),
        }
    }
}

pub trait Annotatable {
    fn annotations<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::Annotation>>;
    // fn annotations_mut(&mut self) -> &mut Option<impl poly_containers::MapRef<'a, String,crate::Annotation>>;
    // fn set_annotations<E>(&mut self, value: Option<&HashMap<String, E>>) where E: Into<Annotation>;
}

impl Annotatable for crate::Annotatable {
    fn annotations<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::Annotation>> {
        return self.annotations.as_ref();
    }
}
impl Annotatable for crate::Annotation {
    fn annotations<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::Annotation>> {
        return self.annotations.as_ref().map(poly_containers::MapView::new);
    }
}
impl Annotatable for crate::Element {
    fn annotations<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::Annotation>> {
        return self.annotations.as_ref();
    }
}
impl Annotatable for crate::EnumBinding {
    fn annotations<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::Annotation>> {
        return self.annotations.as_ref();
    }
}
impl Annotatable for crate::StructuredAlias {
    fn annotations<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::Annotation>> {
        return self.annotations.as_ref();
    }
}
impl Annotatable for crate::AnonymousExpression {
    fn annotations<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::Annotation>> {
        return self.annotations.as_ref();
    }
}
impl Annotatable for crate::PathExpression {
    fn annotations<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::Annotation>> {
        return self.annotations.as_ref();
    }
}
impl Annotatable for crate::ClassRule {
    fn annotations<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::Annotation>> {
        return self.annotations.as_ref();
    }
}
impl Annotatable for crate::ArrayExpression {
    fn annotations<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::Annotation>> {
        return self.annotations.as_ref();
    }
}
impl Annotatable for crate::DimensionExpression {
    fn annotations<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::Annotation>> {
        return self.annotations.as_ref();
    }
}
impl Annotatable for crate::PatternExpression {
    fn annotations<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::Annotation>> {
        return self.annotations.as_ref();
    }
}
impl Annotatable for crate::ImportExpression {
    fn annotations<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::Annotation>> {
        return self.annotations.as_ref();
    }
}
impl Annotatable for crate::PermissibleValue {
    fn annotations<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::Annotation>> {
        return self.annotations.as_ref();
    }
}
impl Annotatable for crate::UniqueKey {
    fn annotations<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::Annotation>> {
        return self.annotations.as_ref();
    }
}
impl Annotatable for crate::TypeMapping {
    fn annotations<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::Annotation>> {
        return self.annotations.as_ref();
    }
}
impl Annotatable for crate::AnonymousSlotExpression {
    fn annotations<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::Annotation>> {
        return self.annotations.as_ref();
    }
}
impl Annotatable for crate::AnonymousClassExpression {
    fn annotations<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::Annotation>> {
        return self.annotations.as_ref();
    }
}
impl Annotatable for crate::SchemaDefinition {
    fn annotations<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::Annotation>> {
        return self.annotations.as_ref();
    }
}
impl Annotatable for crate::TypeDefinition {
    fn annotations<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::Annotation>> {
        return self.annotations.as_ref();
    }
}
impl Annotatable for crate::SubsetDefinition {
    fn annotations<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::Annotation>> {
        return self.annotations.as_ref();
    }
}
impl Annotatable for crate::Definition {
    fn annotations<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::Annotation>> {
        return self.annotations.as_ref();
    }
}
impl Annotatable for crate::EnumDefinition {
    fn annotations<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::Annotation>> {
        return self.annotations.as_ref();
    }
}
impl Annotatable for crate::SlotDefinition {
    fn annotations<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::Annotation>> {
        return self.annotations.as_ref();
    }
}
impl Annotatable for crate::ClassDefinition {
    fn annotations<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::Annotation>> {
        return self.annotations.as_ref();
    }
}

impl Annotatable for crate::AnnotatableOrSubtype {
    fn annotations<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::Annotation>> {
        match self {
            AnnotatableOrSubtype::Annotation(val) => val.annotations().map(|x| x.to_any()),
            AnnotatableOrSubtype::Element(val) => val.annotations().map(|x| x.to_any()),
            AnnotatableOrSubtype::EnumBinding(val) => val.annotations().map(|x| x.to_any()),
            AnnotatableOrSubtype::StructuredAlias(val) => val.annotations().map(|x| x.to_any()),
            AnnotatableOrSubtype::AnonymousExpression(val) => val.annotations().map(|x| x.to_any()),
            AnnotatableOrSubtype::PathExpression(val) => val.annotations().map(|x| x.to_any()),
            AnnotatableOrSubtype::ClassRule(val) => val.annotations().map(|x| x.to_any()),
            AnnotatableOrSubtype::ArrayExpression(val) => val.annotations().map(|x| x.to_any()),
            AnnotatableOrSubtype::DimensionExpression(val) => val.annotations().map(|x| x.to_any()),
            AnnotatableOrSubtype::PatternExpression(val) => val.annotations().map(|x| x.to_any()),
            AnnotatableOrSubtype::ImportExpression(val) => val.annotations().map(|x| x.to_any()),
            AnnotatableOrSubtype::PermissibleValue(val) => val.annotations().map(|x| x.to_any()),
            AnnotatableOrSubtype::UniqueKey(val) => val.annotations().map(|x| x.to_any()),
            AnnotatableOrSubtype::TypeMapping(val) => val.annotations().map(|x| x.to_any()),
            AnnotatableOrSubtype::AnonymousSlotExpression(val) => {
                val.annotations().map(|x| x.to_any())
            }
            AnnotatableOrSubtype::AnonymousClassExpression(val) => {
                val.annotations().map(|x| x.to_any())
            }
            AnnotatableOrSubtype::SchemaDefinition(val) => val.annotations().map(|x| x.to_any()),
            AnnotatableOrSubtype::TypeDefinition(val) => val.annotations().map(|x| x.to_any()),
            AnnotatableOrSubtype::SubsetDefinition(val) => val.annotations().map(|x| x.to_any()),
            AnnotatableOrSubtype::Definition(val) => val.annotations().map(|x| x.to_any()),
            AnnotatableOrSubtype::EnumDefinition(val) => val.annotations().map(|x| x.to_any()),
            AnnotatableOrSubtype::SlotDefinition(val) => val.annotations().map(|x| x.to_any()),
            AnnotatableOrSubtype::ClassDefinition(val) => val.annotations().map(|x| x.to_any()),
        }
    }
}
impl Annotatable for crate::ElementOrSubtype {
    fn annotations<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::Annotation>> {
        match self {
            ElementOrSubtype::SchemaDefinition(val) => val.annotations().map(|x| x.to_any()),
            ElementOrSubtype::TypeDefinition(val) => val.annotations().map(|x| x.to_any()),
            ElementOrSubtype::SubsetDefinition(val) => val.annotations().map(|x| x.to_any()),
            ElementOrSubtype::Definition(val) => val.annotations().map(|x| x.to_any()),
            ElementOrSubtype::EnumDefinition(val) => val.annotations().map(|x| x.to_any()),
            ElementOrSubtype::SlotDefinition(val) => val.annotations().map(|x| x.to_any()),
            ElementOrSubtype::ClassDefinition(val) => val.annotations().map(|x| x.to_any()),
        }
    }
}
impl Annotatable for crate::AnonymousExpressionOrSubtype {
    fn annotations<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::Annotation>> {
        match self {
            AnonymousExpressionOrSubtype::AnonymousSlotExpression(val) => {
                val.annotations().map(|x| x.to_any())
            }
            AnonymousExpressionOrSubtype::AnonymousClassExpression(val) => {
                val.annotations().map(|x| x.to_any())
            }
        }
    }
}
impl Annotatable for crate::DefinitionOrSubtype {
    fn annotations<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::Annotation>> {
        match self {
            DefinitionOrSubtype::EnumDefinition(val) => val.annotations().map(|x| x.to_any()),
            DefinitionOrSubtype::SlotDefinition(val) => val.annotations().map(|x| x.to_any()),
            DefinitionOrSubtype::ClassDefinition(val) => val.annotations().map(|x| x.to_any()),
        }
    }
}

pub trait Annotation: Extension + Annotatable {}

impl Annotation for crate::Annotation {}

pub trait UnitOfMeasure {
    fn symbol<'a>(&'a self) -> Option<&'a str>;
    // fn symbol_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_symbol(&mut self, value: Option<&'a str>);

    fn abbreviation<'a>(&'a self) -> Option<&'a str>;
    // fn abbreviation_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_abbreviation(&mut self, value: Option<&'a str>);

    fn descriptive_name<'a>(&'a self) -> Option<&'a str>;
    // fn descriptive_name_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_descriptive_name(&mut self, value: Option<&'a str>);

    fn exact_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>>;
    // fn exact_mappings_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>>;
    // fn set_exact_mappings(&mut self, value: Option<&Vec<uriorcurie>>);

    fn ucum_code<'a>(&'a self) -> Option<&'a str>;
    // fn ucum_code_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_ucum_code(&mut self, value: Option<&'a str>);

    fn derivation<'a>(&'a self) -> Option<&'a str>;
    // fn derivation_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_derivation(&mut self, value: Option<&'a str>);

    fn has_quantity_kind<'a>(&'a self) -> Option<&'a crate::uriorcurie>;
    // fn has_quantity_kind_mut(&mut self) -> &mut Option<&'a crate::uriorcurie>;
    // fn set_has_quantity_kind(&mut self, value: Option<&'a uriorcurie>);

    fn iec61360code<'a>(&'a self) -> Option<&'a str>;
    // fn iec61360code_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_iec61360code(&mut self, value: Option<&'a str>);
}

impl UnitOfMeasure for crate::UnitOfMeasure {
    fn symbol<'a>(&'a self) -> Option<&'a str> {
        return self.symbol.as_deref();
    }
    fn abbreviation<'a>(&'a self) -> Option<&'a str> {
        return self.abbreviation.as_deref();
    }
    fn descriptive_name<'a>(&'a self) -> Option<&'a str> {
        return self.descriptive_name.as_deref();
    }
    fn exact_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.exact_mappings.as_ref();
    }
    fn ucum_code<'a>(&'a self) -> Option<&'a str> {
        return self.ucum_code.as_deref();
    }
    fn derivation<'a>(&'a self) -> Option<&'a str> {
        return self.derivation.as_deref();
    }
    fn has_quantity_kind<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.has_quantity_kind.as_ref();
    }
    fn iec61360code<'a>(&'a self) -> Option<&'a str> {
        return self.iec61360code.as_deref();
    }
}

pub trait Anything {}

impl Anything for crate::Anything {}

pub trait CommonMetadata {
    fn description<'a>(&'a self) -> Option<&'a str>;
    // fn description_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_description(&mut self, value: Option<&'a str>);

    fn alt_descriptions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::AltDescription>>;
    // fn alt_descriptions_mut(&mut self) -> &mut Option<impl poly_containers::MapRef<'a, String,crate::AltDescription>>;
    // fn set_alt_descriptions<E>(&mut self, value: Option<&HashMap<String, E>>) where E: Into<AltDescription>;

    fn title<'a>(&'a self) -> Option<&'a str>;
    // fn title_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_title(&mut self, value: Option<&'a str>);

    fn deprecated<'a>(&'a self) -> Option<&'a str>;
    // fn deprecated_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_deprecated(&mut self, value: Option<&'a str>);

    fn todos<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>>;
    // fn todos_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, String>>;
    // fn set_todos(&mut self, value: Option<&Vec<String>>);

    fn notes<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>>;
    // fn notes_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, String>>;
    // fn set_notes(&mut self, value: Option<&Vec<String>>);

    fn comments<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>>;
    // fn comments_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, String>>;
    // fn set_comments(&mut self, value: Option<&Vec<String>>);

    fn examples<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::Example>>;
    // fn examples_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, crate::Example>>;
    // fn set_examples<E>(&mut self, value: Option<&Vec<E>>) where E: Into<Example>;

    fn in_subset<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>>;
    // fn in_subset_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, String>>;
    // fn set_in_subset<E>(&mut self, value: Option<&Vec<String>>) where E: Into<String>;

    fn from_schema<'a>(&'a self) -> Option<&'a crate::uri>;
    // fn from_schema_mut(&mut self) -> &mut Option<&'a crate::uri>;
    // fn set_from_schema(&mut self, value: Option<&'a uri>);

    fn imported_from<'a>(&'a self) -> Option<&'a str>;
    // fn imported_from_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_imported_from(&mut self, value: Option<&'a str>);

    fn source<'a>(&'a self) -> Option<&'a crate::uriorcurie>;
    // fn source_mut(&mut self) -> &mut Option<&'a crate::uriorcurie>;
    // fn set_source(&mut self, value: Option<&'a uriorcurie>);

    fn in_language<'a>(&'a self) -> Option<&'a str>;
    // fn in_language_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_in_language(&mut self, value: Option<&'a str>);

    fn see_also<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>>;
    // fn see_also_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>>;
    // fn set_see_also(&mut self, value: Option<&Vec<uriorcurie>>);

    fn deprecated_element_has_exact_replacement<'a>(&'a self) -> Option<&'a crate::uriorcurie>;
    // fn deprecated_element_has_exact_replacement_mut(&mut self) -> &mut Option<&'a crate::uriorcurie>;
    // fn set_deprecated_element_has_exact_replacement(&mut self, value: Option<&'a uriorcurie>);

    fn deprecated_element_has_possible_replacement<'a>(&'a self) -> Option<&'a crate::uriorcurie>;
    // fn deprecated_element_has_possible_replacement_mut(&mut self) -> &mut Option<&'a crate::uriorcurie>;
    // fn set_deprecated_element_has_possible_replacement(&mut self, value: Option<&'a uriorcurie>);

    fn aliases<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>>;
    // fn aliases_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, String>>;
    // fn set_aliases(&mut self, value: Option<&Vec<String>>);

    fn structured_aliases<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::StructuredAlias>>;
    // fn structured_aliases_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, crate::StructuredAlias>>;
    // fn set_structured_aliases<E>(&mut self, value: Option<&Vec<E>>) where E: Into<StructuredAlias>;

    fn mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>>;
    // fn mappings_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>>;
    // fn set_mappings(&mut self, value: Option<&Vec<uriorcurie>>);

    fn exact_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>>;
    // fn exact_mappings_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>>;
    // fn set_exact_mappings(&mut self, value: Option<&Vec<uriorcurie>>);

    fn close_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>>;
    // fn close_mappings_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>>;
    // fn set_close_mappings(&mut self, value: Option<&Vec<uriorcurie>>);

    fn related_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>>;
    // fn related_mappings_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>>;
    // fn set_related_mappings(&mut self, value: Option<&Vec<uriorcurie>>);

    fn narrow_mappings<'a>(&'a self)
        -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>>;
    // fn narrow_mappings_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>>;
    // fn set_narrow_mappings(&mut self, value: Option<&Vec<uriorcurie>>);

    fn broad_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>>;
    // fn broad_mappings_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>>;
    // fn set_broad_mappings(&mut self, value: Option<&Vec<uriorcurie>>);

    fn created_by<'a>(&'a self) -> Option<&'a crate::uriorcurie>;
    // fn created_by_mut(&mut self) -> &mut Option<&'a crate::uriorcurie>;
    // fn set_created_by(&mut self, value: Option<&'a uriorcurie>);

    fn contributors<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>>;
    // fn contributors_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>>;
    // fn set_contributors(&mut self, value: Option<&Vec<uriorcurie>>);

    fn created_on<'a>(&'a self) -> Option<&'a crate::NaiveDateTime>;
    // fn created_on_mut(&mut self) -> &mut Option<&'a crate::NaiveDateTime>;
    // fn set_created_on(&mut self, value: Option<&'a NaiveDateTime>);

    fn last_updated_on<'a>(&'a self) -> Option<&'a crate::NaiveDateTime>;
    // fn last_updated_on_mut(&mut self) -> &mut Option<&'a crate::NaiveDateTime>;
    // fn set_last_updated_on(&mut self, value: Option<&'a NaiveDateTime>);

    fn modified_by<'a>(&'a self) -> Option<&'a crate::uriorcurie>;
    // fn modified_by_mut(&mut self) -> &mut Option<&'a crate::uriorcurie>;
    // fn set_modified_by(&mut self, value: Option<&'a uriorcurie>);

    fn status<'a>(&'a self) -> Option<&'a crate::uriorcurie>;
    // fn status_mut(&mut self) -> &mut Option<&'a crate::uriorcurie>;
    // fn set_status(&mut self, value: Option<&'a uriorcurie>);

    fn rank(&self) -> Option<isize>;
    // fn rank_mut(&mut self) -> &mut Option<isize>;
    // fn set_rank(&mut self, value: Option<isize>);

    fn categories<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>>;
    // fn categories_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>>;
    // fn set_categories(&mut self, value: Option<&Vec<uriorcurie>>);

    fn keywords<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>>;
    // fn keywords_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, String>>;
    // fn set_keywords(&mut self, value: Option<&Vec<String>>);
}

impl CommonMetadata for crate::CommonMetadata {
    fn description<'a>(&'a self) -> Option<&'a str> {
        return self.description.as_deref();
    }
    fn alt_descriptions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::AltDescription>> {
        return self.alt_descriptions.as_ref();
    }
    fn title<'a>(&'a self) -> Option<&'a str> {
        return self.title.as_deref();
    }
    fn deprecated<'a>(&'a self) -> Option<&'a str> {
        return self.deprecated.as_deref();
    }
    fn todos<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.todos.as_ref();
    }
    fn notes<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.notes.as_ref();
    }
    fn comments<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.comments.as_ref();
    }
    fn examples<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::Example>> {
        return self.examples.as_ref();
    }
    fn in_subset<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.in_subset.as_ref();
    }
    fn from_schema<'a>(&'a self) -> Option<&'a crate::uri> {
        return self.from_schema.as_ref();
    }
    fn imported_from<'a>(&'a self) -> Option<&'a str> {
        return self.imported_from.as_deref();
    }
    fn source<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.source.as_ref();
    }
    fn in_language<'a>(&'a self) -> Option<&'a str> {
        return self.in_language.as_deref();
    }
    fn see_also<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.see_also.as_ref();
    }
    fn deprecated_element_has_exact_replacement<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.deprecated_element_has_exact_replacement.as_ref();
    }
    fn deprecated_element_has_possible_replacement<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.deprecated_element_has_possible_replacement.as_ref();
    }
    fn aliases<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.aliases.as_ref();
    }
    fn structured_aliases<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::StructuredAlias>> {
        return self.structured_aliases.as_ref();
    }
    fn mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.mappings.as_ref();
    }
    fn exact_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.exact_mappings.as_ref();
    }
    fn close_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.close_mappings.as_ref();
    }
    fn related_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.related_mappings.as_ref();
    }
    fn narrow_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.narrow_mappings.as_ref();
    }
    fn broad_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.broad_mappings.as_ref();
    }
    fn created_by<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.created_by.as_ref();
    }
    fn contributors<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.contributors.as_ref();
    }
    fn created_on<'a>(&'a self) -> Option<&'a crate::NaiveDateTime> {
        return self.created_on.as_ref();
    }
    fn last_updated_on<'a>(&'a self) -> Option<&'a crate::NaiveDateTime> {
        return self.last_updated_on.as_ref();
    }
    fn modified_by<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.modified_by.as_ref();
    }
    fn status<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.status.as_ref();
    }
    fn rank(&self) -> Option<isize> {
        return self.rank;
    }
    fn categories<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.categories.as_ref();
    }
    fn keywords<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.keywords.as_ref();
    }
}
impl CommonMetadata for crate::Element {
    fn description<'a>(&'a self) -> Option<&'a str> {
        return self.description.as_deref();
    }
    fn alt_descriptions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::AltDescription>> {
        return self.alt_descriptions.as_ref();
    }
    fn title<'a>(&'a self) -> Option<&'a str> {
        return self.title.as_deref();
    }
    fn deprecated<'a>(&'a self) -> Option<&'a str> {
        return self.deprecated.as_deref();
    }
    fn todos<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.todos.as_ref();
    }
    fn notes<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.notes.as_ref();
    }
    fn comments<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.comments.as_ref();
    }
    fn examples<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::Example>> {
        return self.examples.as_ref();
    }
    fn in_subset<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.in_subset.as_ref();
    }
    fn from_schema<'a>(&'a self) -> Option<&'a crate::uri> {
        return self.from_schema.as_ref();
    }
    fn imported_from<'a>(&'a self) -> Option<&'a str> {
        return self.imported_from.as_deref();
    }
    fn source<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.source.as_ref();
    }
    fn in_language<'a>(&'a self) -> Option<&'a str> {
        return self.in_language.as_deref();
    }
    fn see_also<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.see_also.as_ref();
    }
    fn deprecated_element_has_exact_replacement<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.deprecated_element_has_exact_replacement.as_ref();
    }
    fn deprecated_element_has_possible_replacement<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.deprecated_element_has_possible_replacement.as_ref();
    }
    fn aliases<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.aliases.as_ref();
    }
    fn structured_aliases<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::StructuredAlias>> {
        return self.structured_aliases.as_ref();
    }
    fn mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.mappings.as_ref();
    }
    fn exact_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.exact_mappings.as_ref();
    }
    fn close_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.close_mappings.as_ref();
    }
    fn related_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.related_mappings.as_ref();
    }
    fn narrow_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.narrow_mappings.as_ref();
    }
    fn broad_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.broad_mappings.as_ref();
    }
    fn created_by<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.created_by.as_ref();
    }
    fn contributors<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.contributors.as_ref();
    }
    fn created_on<'a>(&'a self) -> Option<&'a crate::NaiveDateTime> {
        return self.created_on.as_ref();
    }
    fn last_updated_on<'a>(&'a self) -> Option<&'a crate::NaiveDateTime> {
        return self.last_updated_on.as_ref();
    }
    fn modified_by<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.modified_by.as_ref();
    }
    fn status<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.status.as_ref();
    }
    fn rank(&self) -> Option<isize> {
        return self.rank;
    }
    fn categories<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.categories.as_ref();
    }
    fn keywords<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.keywords.as_ref();
    }
}
impl CommonMetadata for crate::EnumBinding {
    fn description<'a>(&'a self) -> Option<&'a str> {
        return self.description.as_deref();
    }
    fn alt_descriptions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::AltDescription>> {
        return self.alt_descriptions.as_ref();
    }
    fn title<'a>(&'a self) -> Option<&'a str> {
        return self.title.as_deref();
    }
    fn deprecated<'a>(&'a self) -> Option<&'a str> {
        return self.deprecated.as_deref();
    }
    fn todos<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.todos.as_ref();
    }
    fn notes<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.notes.as_ref();
    }
    fn comments<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.comments.as_ref();
    }
    fn examples<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::Example>> {
        return self.examples.as_ref();
    }
    fn in_subset<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.in_subset.as_ref();
    }
    fn from_schema<'a>(&'a self) -> Option<&'a crate::uri> {
        return self.from_schema.as_ref();
    }
    fn imported_from<'a>(&'a self) -> Option<&'a str> {
        return self.imported_from.as_deref();
    }
    fn source<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.source.as_ref();
    }
    fn in_language<'a>(&'a self) -> Option<&'a str> {
        return self.in_language.as_deref();
    }
    fn see_also<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.see_also.as_ref();
    }
    fn deprecated_element_has_exact_replacement<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.deprecated_element_has_exact_replacement.as_ref();
    }
    fn deprecated_element_has_possible_replacement<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.deprecated_element_has_possible_replacement.as_ref();
    }
    fn aliases<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.aliases.as_ref();
    }
    fn structured_aliases<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::StructuredAlias>> {
        return self.structured_aliases.as_ref();
    }
    fn mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.mappings.as_ref();
    }
    fn exact_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.exact_mappings.as_ref();
    }
    fn close_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.close_mappings.as_ref();
    }
    fn related_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.related_mappings.as_ref();
    }
    fn narrow_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.narrow_mappings.as_ref();
    }
    fn broad_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.broad_mappings.as_ref();
    }
    fn created_by<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.created_by.as_ref();
    }
    fn contributors<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.contributors.as_ref();
    }
    fn created_on<'a>(&'a self) -> Option<&'a crate::NaiveDateTime> {
        return self.created_on.as_ref();
    }
    fn last_updated_on<'a>(&'a self) -> Option<&'a crate::NaiveDateTime> {
        return self.last_updated_on.as_ref();
    }
    fn modified_by<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.modified_by.as_ref();
    }
    fn status<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.status.as_ref();
    }
    fn rank(&self) -> Option<isize> {
        return self.rank;
    }
    fn categories<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.categories.as_ref();
    }
    fn keywords<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.keywords.as_ref();
    }
}
impl CommonMetadata for crate::StructuredAlias {
    fn description<'a>(&'a self) -> Option<&'a str> {
        return self.description.as_deref();
    }
    fn alt_descriptions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::AltDescription>> {
        return self.alt_descriptions.as_ref();
    }
    fn title<'a>(&'a self) -> Option<&'a str> {
        return self.title.as_deref();
    }
    fn deprecated<'a>(&'a self) -> Option<&'a str> {
        return self.deprecated.as_deref();
    }
    fn todos<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.todos.as_ref();
    }
    fn notes<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.notes.as_ref();
    }
    fn comments<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.comments.as_ref();
    }
    fn examples<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::Example>> {
        return self.examples.as_ref();
    }
    fn in_subset<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.in_subset.as_ref();
    }
    fn from_schema<'a>(&'a self) -> Option<&'a crate::uri> {
        return self.from_schema.as_ref();
    }
    fn imported_from<'a>(&'a self) -> Option<&'a str> {
        return self.imported_from.as_deref();
    }
    fn source<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.source.as_ref();
    }
    fn in_language<'a>(&'a self) -> Option<&'a str> {
        return self.in_language.as_deref();
    }
    fn see_also<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.see_also.as_ref();
    }
    fn deprecated_element_has_exact_replacement<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.deprecated_element_has_exact_replacement.as_ref();
    }
    fn deprecated_element_has_possible_replacement<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.deprecated_element_has_possible_replacement.as_ref();
    }
    fn aliases<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.aliases.as_ref();
    }
    fn structured_aliases<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::StructuredAlias>> {
        return self
            .structured_aliases
            .as_ref()
            .map(|x| poly_containers::ListView::new(x));
    }
    fn mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.mappings.as_ref();
    }
    fn exact_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.exact_mappings.as_ref();
    }
    fn close_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.close_mappings.as_ref();
    }
    fn related_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.related_mappings.as_ref();
    }
    fn narrow_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.narrow_mappings.as_ref();
    }
    fn broad_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.broad_mappings.as_ref();
    }
    fn created_by<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.created_by.as_ref();
    }
    fn contributors<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.contributors.as_ref();
    }
    fn created_on<'a>(&'a self) -> Option<&'a crate::NaiveDateTime> {
        return self.created_on.as_ref();
    }
    fn last_updated_on<'a>(&'a self) -> Option<&'a crate::NaiveDateTime> {
        return self.last_updated_on.as_ref();
    }
    fn modified_by<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.modified_by.as_ref();
    }
    fn status<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.status.as_ref();
    }
    fn rank(&self) -> Option<isize> {
        return self.rank;
    }
    fn categories<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.categories.as_ref();
    }
    fn keywords<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.keywords.as_ref();
    }
}
impl CommonMetadata for crate::AnonymousExpression {
    fn description<'a>(&'a self) -> Option<&'a str> {
        return self.description.as_deref();
    }
    fn alt_descriptions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::AltDescription>> {
        return self.alt_descriptions.as_ref();
    }
    fn title<'a>(&'a self) -> Option<&'a str> {
        return self.title.as_deref();
    }
    fn deprecated<'a>(&'a self) -> Option<&'a str> {
        return self.deprecated.as_deref();
    }
    fn todos<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.todos.as_ref();
    }
    fn notes<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.notes.as_ref();
    }
    fn comments<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.comments.as_ref();
    }
    fn examples<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::Example>> {
        return self.examples.as_ref();
    }
    fn in_subset<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.in_subset.as_ref();
    }
    fn from_schema<'a>(&'a self) -> Option<&'a crate::uri> {
        return self.from_schema.as_ref();
    }
    fn imported_from<'a>(&'a self) -> Option<&'a str> {
        return self.imported_from.as_deref();
    }
    fn source<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.source.as_ref();
    }
    fn in_language<'a>(&'a self) -> Option<&'a str> {
        return self.in_language.as_deref();
    }
    fn see_also<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.see_also.as_ref();
    }
    fn deprecated_element_has_exact_replacement<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.deprecated_element_has_exact_replacement.as_ref();
    }
    fn deprecated_element_has_possible_replacement<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.deprecated_element_has_possible_replacement.as_ref();
    }
    fn aliases<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.aliases.as_ref();
    }
    fn structured_aliases<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::StructuredAlias>> {
        return self.structured_aliases.as_ref();
    }
    fn mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.mappings.as_ref();
    }
    fn exact_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.exact_mappings.as_ref();
    }
    fn close_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.close_mappings.as_ref();
    }
    fn related_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.related_mappings.as_ref();
    }
    fn narrow_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.narrow_mappings.as_ref();
    }
    fn broad_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.broad_mappings.as_ref();
    }
    fn created_by<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.created_by.as_ref();
    }
    fn contributors<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.contributors.as_ref();
    }
    fn created_on<'a>(&'a self) -> Option<&'a crate::NaiveDateTime> {
        return self.created_on.as_ref();
    }
    fn last_updated_on<'a>(&'a self) -> Option<&'a crate::NaiveDateTime> {
        return self.last_updated_on.as_ref();
    }
    fn modified_by<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.modified_by.as_ref();
    }
    fn status<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.status.as_ref();
    }
    fn rank(&self) -> Option<isize> {
        return self.rank;
    }
    fn categories<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.categories.as_ref();
    }
    fn keywords<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.keywords.as_ref();
    }
}
impl CommonMetadata for crate::PathExpression {
    fn description<'a>(&'a self) -> Option<&'a str> {
        return self.description.as_deref();
    }
    fn alt_descriptions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::AltDescription>> {
        return self.alt_descriptions.as_ref();
    }
    fn title<'a>(&'a self) -> Option<&'a str> {
        return self.title.as_deref();
    }
    fn deprecated<'a>(&'a self) -> Option<&'a str> {
        return self.deprecated.as_deref();
    }
    fn todos<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.todos.as_ref();
    }
    fn notes<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.notes.as_ref();
    }
    fn comments<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.comments.as_ref();
    }
    fn examples<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::Example>> {
        return self.examples.as_ref();
    }
    fn in_subset<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.in_subset.as_ref();
    }
    fn from_schema<'a>(&'a self) -> Option<&'a crate::uri> {
        return self.from_schema.as_ref();
    }
    fn imported_from<'a>(&'a self) -> Option<&'a str> {
        return self.imported_from.as_deref();
    }
    fn source<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.source.as_ref();
    }
    fn in_language<'a>(&'a self) -> Option<&'a str> {
        return self.in_language.as_deref();
    }
    fn see_also<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.see_also.as_ref();
    }
    fn deprecated_element_has_exact_replacement<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.deprecated_element_has_exact_replacement.as_ref();
    }
    fn deprecated_element_has_possible_replacement<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.deprecated_element_has_possible_replacement.as_ref();
    }
    fn aliases<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.aliases.as_ref();
    }
    fn structured_aliases<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::StructuredAlias>> {
        return self.structured_aliases.as_ref();
    }
    fn mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.mappings.as_ref();
    }
    fn exact_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.exact_mappings.as_ref();
    }
    fn close_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.close_mappings.as_ref();
    }
    fn related_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.related_mappings.as_ref();
    }
    fn narrow_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.narrow_mappings.as_ref();
    }
    fn broad_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.broad_mappings.as_ref();
    }
    fn created_by<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.created_by.as_ref();
    }
    fn contributors<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.contributors.as_ref();
    }
    fn created_on<'a>(&'a self) -> Option<&'a crate::NaiveDateTime> {
        return self.created_on.as_ref();
    }
    fn last_updated_on<'a>(&'a self) -> Option<&'a crate::NaiveDateTime> {
        return self.last_updated_on.as_ref();
    }
    fn modified_by<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.modified_by.as_ref();
    }
    fn status<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.status.as_ref();
    }
    fn rank(&self) -> Option<isize> {
        return self.rank;
    }
    fn categories<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.categories.as_ref();
    }
    fn keywords<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.keywords.as_ref();
    }
}
impl CommonMetadata for crate::ClassRule {
    fn description<'a>(&'a self) -> Option<&'a str> {
        return self.description.as_deref();
    }
    fn alt_descriptions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::AltDescription>> {
        return self.alt_descriptions.as_ref();
    }
    fn title<'a>(&'a self) -> Option<&'a str> {
        return self.title.as_deref();
    }
    fn deprecated<'a>(&'a self) -> Option<&'a str> {
        return self.deprecated.as_deref();
    }
    fn todos<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.todos.as_ref();
    }
    fn notes<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.notes.as_ref();
    }
    fn comments<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.comments.as_ref();
    }
    fn examples<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::Example>> {
        return self.examples.as_ref();
    }
    fn in_subset<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.in_subset.as_ref();
    }
    fn from_schema<'a>(&'a self) -> Option<&'a crate::uri> {
        return self.from_schema.as_ref();
    }
    fn imported_from<'a>(&'a self) -> Option<&'a str> {
        return self.imported_from.as_deref();
    }
    fn source<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.source.as_ref();
    }
    fn in_language<'a>(&'a self) -> Option<&'a str> {
        return self.in_language.as_deref();
    }
    fn see_also<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.see_also.as_ref();
    }
    fn deprecated_element_has_exact_replacement<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.deprecated_element_has_exact_replacement.as_ref();
    }
    fn deprecated_element_has_possible_replacement<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.deprecated_element_has_possible_replacement.as_ref();
    }
    fn aliases<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.aliases.as_ref();
    }
    fn structured_aliases<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::StructuredAlias>> {
        return self.structured_aliases.as_ref();
    }
    fn mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.mappings.as_ref();
    }
    fn exact_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.exact_mappings.as_ref();
    }
    fn close_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.close_mappings.as_ref();
    }
    fn related_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.related_mappings.as_ref();
    }
    fn narrow_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.narrow_mappings.as_ref();
    }
    fn broad_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.broad_mappings.as_ref();
    }
    fn created_by<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.created_by.as_ref();
    }
    fn contributors<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.contributors.as_ref();
    }
    fn created_on<'a>(&'a self) -> Option<&'a crate::NaiveDateTime> {
        return self.created_on.as_ref();
    }
    fn last_updated_on<'a>(&'a self) -> Option<&'a crate::NaiveDateTime> {
        return self.last_updated_on.as_ref();
    }
    fn modified_by<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.modified_by.as_ref();
    }
    fn status<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.status.as_ref();
    }
    fn rank(&self) -> Option<isize> {
        return self.rank;
    }
    fn categories<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.categories.as_ref();
    }
    fn keywords<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.keywords.as_ref();
    }
}
impl CommonMetadata for crate::ArrayExpression {
    fn description<'a>(&'a self) -> Option<&'a str> {
        return self.description.as_deref();
    }
    fn alt_descriptions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::AltDescription>> {
        return self.alt_descriptions.as_ref();
    }
    fn title<'a>(&'a self) -> Option<&'a str> {
        return self.title.as_deref();
    }
    fn deprecated<'a>(&'a self) -> Option<&'a str> {
        return self.deprecated.as_deref();
    }
    fn todos<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.todos.as_ref();
    }
    fn notes<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.notes.as_ref();
    }
    fn comments<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.comments.as_ref();
    }
    fn examples<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::Example>> {
        return self.examples.as_ref();
    }
    fn in_subset<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.in_subset.as_ref();
    }
    fn from_schema<'a>(&'a self) -> Option<&'a crate::uri> {
        return self.from_schema.as_ref();
    }
    fn imported_from<'a>(&'a self) -> Option<&'a str> {
        return self.imported_from.as_deref();
    }
    fn source<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.source.as_ref();
    }
    fn in_language<'a>(&'a self) -> Option<&'a str> {
        return self.in_language.as_deref();
    }
    fn see_also<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.see_also.as_ref();
    }
    fn deprecated_element_has_exact_replacement<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.deprecated_element_has_exact_replacement.as_ref();
    }
    fn deprecated_element_has_possible_replacement<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.deprecated_element_has_possible_replacement.as_ref();
    }
    fn aliases<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.aliases.as_ref();
    }
    fn structured_aliases<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::StructuredAlias>> {
        return self.structured_aliases.as_ref();
    }
    fn mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.mappings.as_ref();
    }
    fn exact_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.exact_mappings.as_ref();
    }
    fn close_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.close_mappings.as_ref();
    }
    fn related_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.related_mappings.as_ref();
    }
    fn narrow_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.narrow_mappings.as_ref();
    }
    fn broad_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.broad_mappings.as_ref();
    }
    fn created_by<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.created_by.as_ref();
    }
    fn contributors<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.contributors.as_ref();
    }
    fn created_on<'a>(&'a self) -> Option<&'a crate::NaiveDateTime> {
        return self.created_on.as_ref();
    }
    fn last_updated_on<'a>(&'a self) -> Option<&'a crate::NaiveDateTime> {
        return self.last_updated_on.as_ref();
    }
    fn modified_by<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.modified_by.as_ref();
    }
    fn status<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.status.as_ref();
    }
    fn rank(&self) -> Option<isize> {
        return self.rank;
    }
    fn categories<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.categories.as_ref();
    }
    fn keywords<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.keywords.as_ref();
    }
}
impl CommonMetadata for crate::DimensionExpression {
    fn description<'a>(&'a self) -> Option<&'a str> {
        return self.description.as_deref();
    }
    fn alt_descriptions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::AltDescription>> {
        return self.alt_descriptions.as_ref();
    }
    fn title<'a>(&'a self) -> Option<&'a str> {
        return self.title.as_deref();
    }
    fn deprecated<'a>(&'a self) -> Option<&'a str> {
        return self.deprecated.as_deref();
    }
    fn todos<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.todos.as_ref();
    }
    fn notes<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.notes.as_ref();
    }
    fn comments<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.comments.as_ref();
    }
    fn examples<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::Example>> {
        return self.examples.as_ref();
    }
    fn in_subset<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.in_subset.as_ref();
    }
    fn from_schema<'a>(&'a self) -> Option<&'a crate::uri> {
        return self.from_schema.as_ref();
    }
    fn imported_from<'a>(&'a self) -> Option<&'a str> {
        return self.imported_from.as_deref();
    }
    fn source<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.source.as_ref();
    }
    fn in_language<'a>(&'a self) -> Option<&'a str> {
        return self.in_language.as_deref();
    }
    fn see_also<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.see_also.as_ref();
    }
    fn deprecated_element_has_exact_replacement<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.deprecated_element_has_exact_replacement.as_ref();
    }
    fn deprecated_element_has_possible_replacement<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.deprecated_element_has_possible_replacement.as_ref();
    }
    fn aliases<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.aliases.as_ref();
    }
    fn structured_aliases<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::StructuredAlias>> {
        return self.structured_aliases.as_ref();
    }
    fn mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.mappings.as_ref();
    }
    fn exact_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.exact_mappings.as_ref();
    }
    fn close_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.close_mappings.as_ref();
    }
    fn related_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.related_mappings.as_ref();
    }
    fn narrow_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.narrow_mappings.as_ref();
    }
    fn broad_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.broad_mappings.as_ref();
    }
    fn created_by<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.created_by.as_ref();
    }
    fn contributors<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.contributors.as_ref();
    }
    fn created_on<'a>(&'a self) -> Option<&'a crate::NaiveDateTime> {
        return self.created_on.as_ref();
    }
    fn last_updated_on<'a>(&'a self) -> Option<&'a crate::NaiveDateTime> {
        return self.last_updated_on.as_ref();
    }
    fn modified_by<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.modified_by.as_ref();
    }
    fn status<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.status.as_ref();
    }
    fn rank(&self) -> Option<isize> {
        return self.rank;
    }
    fn categories<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.categories.as_ref();
    }
    fn keywords<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.keywords.as_ref();
    }
}
impl CommonMetadata for crate::PatternExpression {
    fn description<'a>(&'a self) -> Option<&'a str> {
        return self.description.as_deref();
    }
    fn alt_descriptions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::AltDescription>> {
        return self.alt_descriptions.as_ref();
    }
    fn title<'a>(&'a self) -> Option<&'a str> {
        return self.title.as_deref();
    }
    fn deprecated<'a>(&'a self) -> Option<&'a str> {
        return self.deprecated.as_deref();
    }
    fn todos<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.todos.as_ref();
    }
    fn notes<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.notes.as_ref();
    }
    fn comments<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.comments.as_ref();
    }
    fn examples<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::Example>> {
        return self.examples.as_ref();
    }
    fn in_subset<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.in_subset.as_ref();
    }
    fn from_schema<'a>(&'a self) -> Option<&'a crate::uri> {
        return self.from_schema.as_ref();
    }
    fn imported_from<'a>(&'a self) -> Option<&'a str> {
        return self.imported_from.as_deref();
    }
    fn source<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.source.as_ref();
    }
    fn in_language<'a>(&'a self) -> Option<&'a str> {
        return self.in_language.as_deref();
    }
    fn see_also<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.see_also.as_ref();
    }
    fn deprecated_element_has_exact_replacement<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.deprecated_element_has_exact_replacement.as_ref();
    }
    fn deprecated_element_has_possible_replacement<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.deprecated_element_has_possible_replacement.as_ref();
    }
    fn aliases<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.aliases.as_ref();
    }
    fn structured_aliases<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::StructuredAlias>> {
        return self.structured_aliases.as_ref();
    }
    fn mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.mappings.as_ref();
    }
    fn exact_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.exact_mappings.as_ref();
    }
    fn close_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.close_mappings.as_ref();
    }
    fn related_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.related_mappings.as_ref();
    }
    fn narrow_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.narrow_mappings.as_ref();
    }
    fn broad_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.broad_mappings.as_ref();
    }
    fn created_by<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.created_by.as_ref();
    }
    fn contributors<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.contributors.as_ref();
    }
    fn created_on<'a>(&'a self) -> Option<&'a crate::NaiveDateTime> {
        return self.created_on.as_ref();
    }
    fn last_updated_on<'a>(&'a self) -> Option<&'a crate::NaiveDateTime> {
        return self.last_updated_on.as_ref();
    }
    fn modified_by<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.modified_by.as_ref();
    }
    fn status<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.status.as_ref();
    }
    fn rank(&self) -> Option<isize> {
        return self.rank;
    }
    fn categories<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.categories.as_ref();
    }
    fn keywords<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.keywords.as_ref();
    }
}
impl CommonMetadata for crate::ImportExpression {
    fn description<'a>(&'a self) -> Option<&'a str> {
        return self.description.as_deref();
    }
    fn alt_descriptions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::AltDescription>> {
        return self.alt_descriptions.as_ref();
    }
    fn title<'a>(&'a self) -> Option<&'a str> {
        return self.title.as_deref();
    }
    fn deprecated<'a>(&'a self) -> Option<&'a str> {
        return self.deprecated.as_deref();
    }
    fn todos<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.todos.as_ref();
    }
    fn notes<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.notes.as_ref();
    }
    fn comments<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.comments.as_ref();
    }
    fn examples<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::Example>> {
        return self.examples.as_ref();
    }
    fn in_subset<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.in_subset.as_ref();
    }
    fn from_schema<'a>(&'a self) -> Option<&'a crate::uri> {
        return self.from_schema.as_ref();
    }
    fn imported_from<'a>(&'a self) -> Option<&'a str> {
        return self.imported_from.as_deref();
    }
    fn source<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.source.as_ref();
    }
    fn in_language<'a>(&'a self) -> Option<&'a str> {
        return self.in_language.as_deref();
    }
    fn see_also<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.see_also.as_ref();
    }
    fn deprecated_element_has_exact_replacement<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.deprecated_element_has_exact_replacement.as_ref();
    }
    fn deprecated_element_has_possible_replacement<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.deprecated_element_has_possible_replacement.as_ref();
    }
    fn aliases<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.aliases.as_ref();
    }
    fn structured_aliases<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::StructuredAlias>> {
        return self.structured_aliases.as_ref();
    }
    fn mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.mappings.as_ref();
    }
    fn exact_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.exact_mappings.as_ref();
    }
    fn close_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.close_mappings.as_ref();
    }
    fn related_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.related_mappings.as_ref();
    }
    fn narrow_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.narrow_mappings.as_ref();
    }
    fn broad_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.broad_mappings.as_ref();
    }
    fn created_by<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.created_by.as_ref();
    }
    fn contributors<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.contributors.as_ref();
    }
    fn created_on<'a>(&'a self) -> Option<&'a crate::NaiveDateTime> {
        return self.created_on.as_ref();
    }
    fn last_updated_on<'a>(&'a self) -> Option<&'a crate::NaiveDateTime> {
        return self.last_updated_on.as_ref();
    }
    fn modified_by<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.modified_by.as_ref();
    }
    fn status<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.status.as_ref();
    }
    fn rank(&self) -> Option<isize> {
        return self.rank;
    }
    fn categories<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.categories.as_ref();
    }
    fn keywords<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.keywords.as_ref();
    }
}
impl CommonMetadata for crate::PermissibleValue {
    fn description<'a>(&'a self) -> Option<&'a str> {
        return self.description.as_deref();
    }
    fn alt_descriptions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::AltDescription>> {
        return self.alt_descriptions.as_ref();
    }
    fn title<'a>(&'a self) -> Option<&'a str> {
        return self.title.as_deref();
    }
    fn deprecated<'a>(&'a self) -> Option<&'a str> {
        return self.deprecated.as_deref();
    }
    fn todos<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.todos.as_ref();
    }
    fn notes<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.notes.as_ref();
    }
    fn comments<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.comments.as_ref();
    }
    fn examples<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::Example>> {
        return self.examples.as_ref();
    }
    fn in_subset<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.in_subset.as_ref();
    }
    fn from_schema<'a>(&'a self) -> Option<&'a crate::uri> {
        return self.from_schema.as_ref();
    }
    fn imported_from<'a>(&'a self) -> Option<&'a str> {
        return self.imported_from.as_deref();
    }
    fn source<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.source.as_ref();
    }
    fn in_language<'a>(&'a self) -> Option<&'a str> {
        return self.in_language.as_deref();
    }
    fn see_also<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.see_also.as_ref();
    }
    fn deprecated_element_has_exact_replacement<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.deprecated_element_has_exact_replacement.as_ref();
    }
    fn deprecated_element_has_possible_replacement<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.deprecated_element_has_possible_replacement.as_ref();
    }
    fn aliases<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.aliases.as_ref();
    }
    fn structured_aliases<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::StructuredAlias>> {
        return self.structured_aliases.as_ref();
    }
    fn mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.mappings.as_ref();
    }
    fn exact_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.exact_mappings.as_ref();
    }
    fn close_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.close_mappings.as_ref();
    }
    fn related_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.related_mappings.as_ref();
    }
    fn narrow_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.narrow_mappings.as_ref();
    }
    fn broad_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.broad_mappings.as_ref();
    }
    fn created_by<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.created_by.as_ref();
    }
    fn contributors<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.contributors.as_ref();
    }
    fn created_on<'a>(&'a self) -> Option<&'a crate::NaiveDateTime> {
        return self.created_on.as_ref();
    }
    fn last_updated_on<'a>(&'a self) -> Option<&'a crate::NaiveDateTime> {
        return self.last_updated_on.as_ref();
    }
    fn modified_by<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.modified_by.as_ref();
    }
    fn status<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.status.as_ref();
    }
    fn rank(&self) -> Option<isize> {
        return self.rank;
    }
    fn categories<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.categories.as_ref();
    }
    fn keywords<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.keywords.as_ref();
    }
}
impl CommonMetadata for crate::UniqueKey {
    fn description<'a>(&'a self) -> Option<&'a str> {
        return self.description.as_deref();
    }
    fn alt_descriptions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::AltDescription>> {
        return self.alt_descriptions.as_ref();
    }
    fn title<'a>(&'a self) -> Option<&'a str> {
        return self.title.as_deref();
    }
    fn deprecated<'a>(&'a self) -> Option<&'a str> {
        return self.deprecated.as_deref();
    }
    fn todos<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.todos.as_ref();
    }
    fn notes<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.notes.as_ref();
    }
    fn comments<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.comments.as_ref();
    }
    fn examples<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::Example>> {
        return self.examples.as_ref();
    }
    fn in_subset<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.in_subset.as_ref();
    }
    fn from_schema<'a>(&'a self) -> Option<&'a crate::uri> {
        return self.from_schema.as_ref();
    }
    fn imported_from<'a>(&'a self) -> Option<&'a str> {
        return self.imported_from.as_deref();
    }
    fn source<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.source.as_ref();
    }
    fn in_language<'a>(&'a self) -> Option<&'a str> {
        return self.in_language.as_deref();
    }
    fn see_also<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.see_also.as_ref();
    }
    fn deprecated_element_has_exact_replacement<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.deprecated_element_has_exact_replacement.as_ref();
    }
    fn deprecated_element_has_possible_replacement<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.deprecated_element_has_possible_replacement.as_ref();
    }
    fn aliases<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.aliases.as_ref();
    }
    fn structured_aliases<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::StructuredAlias>> {
        return self.structured_aliases.as_ref();
    }
    fn mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.mappings.as_ref();
    }
    fn exact_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.exact_mappings.as_ref();
    }
    fn close_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.close_mappings.as_ref();
    }
    fn related_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.related_mappings.as_ref();
    }
    fn narrow_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.narrow_mappings.as_ref();
    }
    fn broad_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.broad_mappings.as_ref();
    }
    fn created_by<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.created_by.as_ref();
    }
    fn contributors<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.contributors.as_ref();
    }
    fn created_on<'a>(&'a self) -> Option<&'a crate::NaiveDateTime> {
        return self.created_on.as_ref();
    }
    fn last_updated_on<'a>(&'a self) -> Option<&'a crate::NaiveDateTime> {
        return self.last_updated_on.as_ref();
    }
    fn modified_by<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.modified_by.as_ref();
    }
    fn status<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.status.as_ref();
    }
    fn rank(&self) -> Option<isize> {
        return self.rank;
    }
    fn categories<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.categories.as_ref();
    }
    fn keywords<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.keywords.as_ref();
    }
}
impl CommonMetadata for crate::TypeMapping {
    fn description<'a>(&'a self) -> Option<&'a str> {
        return self.description.as_deref();
    }
    fn alt_descriptions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::AltDescription>> {
        return self.alt_descriptions.as_ref();
    }
    fn title<'a>(&'a self) -> Option<&'a str> {
        return self.title.as_deref();
    }
    fn deprecated<'a>(&'a self) -> Option<&'a str> {
        return self.deprecated.as_deref();
    }
    fn todos<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.todos.as_ref();
    }
    fn notes<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.notes.as_ref();
    }
    fn comments<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.comments.as_ref();
    }
    fn examples<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::Example>> {
        return self.examples.as_ref();
    }
    fn in_subset<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.in_subset.as_ref();
    }
    fn from_schema<'a>(&'a self) -> Option<&'a crate::uri> {
        return self.from_schema.as_ref();
    }
    fn imported_from<'a>(&'a self) -> Option<&'a str> {
        return self.imported_from.as_deref();
    }
    fn source<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.source.as_ref();
    }
    fn in_language<'a>(&'a self) -> Option<&'a str> {
        return self.in_language.as_deref();
    }
    fn see_also<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.see_also.as_ref();
    }
    fn deprecated_element_has_exact_replacement<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.deprecated_element_has_exact_replacement.as_ref();
    }
    fn deprecated_element_has_possible_replacement<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.deprecated_element_has_possible_replacement.as_ref();
    }
    fn aliases<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.aliases.as_ref();
    }
    fn structured_aliases<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::StructuredAlias>> {
        return self.structured_aliases.as_ref();
    }
    fn mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.mappings.as_ref();
    }
    fn exact_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.exact_mappings.as_ref();
    }
    fn close_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.close_mappings.as_ref();
    }
    fn related_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.related_mappings.as_ref();
    }
    fn narrow_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.narrow_mappings.as_ref();
    }
    fn broad_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.broad_mappings.as_ref();
    }
    fn created_by<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.created_by.as_ref();
    }
    fn contributors<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.contributors.as_ref();
    }
    fn created_on<'a>(&'a self) -> Option<&'a crate::NaiveDateTime> {
        return self.created_on.as_ref();
    }
    fn last_updated_on<'a>(&'a self) -> Option<&'a crate::NaiveDateTime> {
        return self.last_updated_on.as_ref();
    }
    fn modified_by<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.modified_by.as_ref();
    }
    fn status<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.status.as_ref();
    }
    fn rank(&self) -> Option<isize> {
        return self.rank;
    }
    fn categories<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.categories.as_ref();
    }
    fn keywords<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.keywords.as_ref();
    }
}
impl CommonMetadata for crate::AnonymousSlotExpression {
    fn description<'a>(&'a self) -> Option<&'a str> {
        return self.description.as_deref();
    }
    fn alt_descriptions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::AltDescription>> {
        return self.alt_descriptions.as_ref();
    }
    fn title<'a>(&'a self) -> Option<&'a str> {
        return self.title.as_deref();
    }
    fn deprecated<'a>(&'a self) -> Option<&'a str> {
        return self.deprecated.as_deref();
    }
    fn todos<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.todos.as_ref();
    }
    fn notes<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.notes.as_ref();
    }
    fn comments<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.comments.as_ref();
    }
    fn examples<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::Example>> {
        return self.examples.as_ref();
    }
    fn in_subset<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.in_subset.as_ref();
    }
    fn from_schema<'a>(&'a self) -> Option<&'a crate::uri> {
        return self.from_schema.as_ref();
    }
    fn imported_from<'a>(&'a self) -> Option<&'a str> {
        return self.imported_from.as_deref();
    }
    fn source<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.source.as_ref();
    }
    fn in_language<'a>(&'a self) -> Option<&'a str> {
        return self.in_language.as_deref();
    }
    fn see_also<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.see_also.as_ref();
    }
    fn deprecated_element_has_exact_replacement<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.deprecated_element_has_exact_replacement.as_ref();
    }
    fn deprecated_element_has_possible_replacement<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.deprecated_element_has_possible_replacement.as_ref();
    }
    fn aliases<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.aliases.as_ref();
    }
    fn structured_aliases<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::StructuredAlias>> {
        return self.structured_aliases.as_ref();
    }
    fn mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.mappings.as_ref();
    }
    fn exact_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.exact_mappings.as_ref();
    }
    fn close_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.close_mappings.as_ref();
    }
    fn related_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.related_mappings.as_ref();
    }
    fn narrow_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.narrow_mappings.as_ref();
    }
    fn broad_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.broad_mappings.as_ref();
    }
    fn created_by<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.created_by.as_ref();
    }
    fn contributors<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.contributors.as_ref();
    }
    fn created_on<'a>(&'a self) -> Option<&'a crate::NaiveDateTime> {
        return self.created_on.as_ref();
    }
    fn last_updated_on<'a>(&'a self) -> Option<&'a crate::NaiveDateTime> {
        return self.last_updated_on.as_ref();
    }
    fn modified_by<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.modified_by.as_ref();
    }
    fn status<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.status.as_ref();
    }
    fn rank(&self) -> Option<isize> {
        return self.rank;
    }
    fn categories<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.categories.as_ref();
    }
    fn keywords<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.keywords.as_ref();
    }
}
impl CommonMetadata for crate::AnonymousClassExpression {
    fn description<'a>(&'a self) -> Option<&'a str> {
        return self.description.as_deref();
    }
    fn alt_descriptions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::AltDescription>> {
        return self.alt_descriptions.as_ref();
    }
    fn title<'a>(&'a self) -> Option<&'a str> {
        return self.title.as_deref();
    }
    fn deprecated<'a>(&'a self) -> Option<&'a str> {
        return self.deprecated.as_deref();
    }
    fn todos<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.todos.as_ref();
    }
    fn notes<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.notes.as_ref();
    }
    fn comments<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.comments.as_ref();
    }
    fn examples<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::Example>> {
        return self.examples.as_ref();
    }
    fn in_subset<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.in_subset.as_ref();
    }
    fn from_schema<'a>(&'a self) -> Option<&'a crate::uri> {
        return self.from_schema.as_ref();
    }
    fn imported_from<'a>(&'a self) -> Option<&'a str> {
        return self.imported_from.as_deref();
    }
    fn source<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.source.as_ref();
    }
    fn in_language<'a>(&'a self) -> Option<&'a str> {
        return self.in_language.as_deref();
    }
    fn see_also<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.see_also.as_ref();
    }
    fn deprecated_element_has_exact_replacement<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.deprecated_element_has_exact_replacement.as_ref();
    }
    fn deprecated_element_has_possible_replacement<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.deprecated_element_has_possible_replacement.as_ref();
    }
    fn aliases<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.aliases.as_ref();
    }
    fn structured_aliases<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::StructuredAlias>> {
        return self.structured_aliases.as_ref();
    }
    fn mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.mappings.as_ref();
    }
    fn exact_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.exact_mappings.as_ref();
    }
    fn close_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.close_mappings.as_ref();
    }
    fn related_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.related_mappings.as_ref();
    }
    fn narrow_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.narrow_mappings.as_ref();
    }
    fn broad_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.broad_mappings.as_ref();
    }
    fn created_by<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.created_by.as_ref();
    }
    fn contributors<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.contributors.as_ref();
    }
    fn created_on<'a>(&'a self) -> Option<&'a crate::NaiveDateTime> {
        return self.created_on.as_ref();
    }
    fn last_updated_on<'a>(&'a self) -> Option<&'a crate::NaiveDateTime> {
        return self.last_updated_on.as_ref();
    }
    fn modified_by<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.modified_by.as_ref();
    }
    fn status<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.status.as_ref();
    }
    fn rank(&self) -> Option<isize> {
        return self.rank;
    }
    fn categories<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.categories.as_ref();
    }
    fn keywords<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.keywords.as_ref();
    }
}
impl CommonMetadata for crate::SchemaDefinition {
    fn description<'a>(&'a self) -> Option<&'a str> {
        return self.description.as_deref();
    }
    fn alt_descriptions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::AltDescription>> {
        return self.alt_descriptions.as_ref();
    }
    fn title<'a>(&'a self) -> Option<&'a str> {
        return self.title.as_deref();
    }
    fn deprecated<'a>(&'a self) -> Option<&'a str> {
        return self.deprecated.as_deref();
    }
    fn todos<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.todos.as_ref();
    }
    fn notes<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.notes.as_ref();
    }
    fn comments<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.comments.as_ref();
    }
    fn examples<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::Example>> {
        return self.examples.as_ref();
    }
    fn in_subset<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.in_subset.as_ref();
    }
    fn from_schema<'a>(&'a self) -> Option<&'a crate::uri> {
        return self.from_schema.as_ref();
    }
    fn imported_from<'a>(&'a self) -> Option<&'a str> {
        return self.imported_from.as_deref();
    }
    fn source<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.source.as_ref();
    }
    fn in_language<'a>(&'a self) -> Option<&'a str> {
        return self.in_language.as_deref();
    }
    fn see_also<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.see_also.as_ref();
    }
    fn deprecated_element_has_exact_replacement<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.deprecated_element_has_exact_replacement.as_ref();
    }
    fn deprecated_element_has_possible_replacement<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.deprecated_element_has_possible_replacement.as_ref();
    }
    fn aliases<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.aliases.as_ref();
    }
    fn structured_aliases<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::StructuredAlias>> {
        return self.structured_aliases.as_ref();
    }
    fn mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.mappings.as_ref();
    }
    fn exact_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.exact_mappings.as_ref();
    }
    fn close_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.close_mappings.as_ref();
    }
    fn related_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.related_mappings.as_ref();
    }
    fn narrow_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.narrow_mappings.as_ref();
    }
    fn broad_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.broad_mappings.as_ref();
    }
    fn created_by<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.created_by.as_ref();
    }
    fn contributors<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.contributors.as_ref();
    }
    fn created_on<'a>(&'a self) -> Option<&'a crate::NaiveDateTime> {
        return self.created_on.as_ref();
    }
    fn last_updated_on<'a>(&'a self) -> Option<&'a crate::NaiveDateTime> {
        return self.last_updated_on.as_ref();
    }
    fn modified_by<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.modified_by.as_ref();
    }
    fn status<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.status.as_ref();
    }
    fn rank(&self) -> Option<isize> {
        return self.rank;
    }
    fn categories<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.categories.as_ref();
    }
    fn keywords<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.keywords.as_ref();
    }
}
impl CommonMetadata for crate::TypeDefinition {
    fn description<'a>(&'a self) -> Option<&'a str> {
        return self.description.as_deref();
    }
    fn alt_descriptions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::AltDescription>> {
        return self.alt_descriptions.as_ref();
    }
    fn title<'a>(&'a self) -> Option<&'a str> {
        return self.title.as_deref();
    }
    fn deprecated<'a>(&'a self) -> Option<&'a str> {
        return self.deprecated.as_deref();
    }
    fn todos<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.todos.as_ref();
    }
    fn notes<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.notes.as_ref();
    }
    fn comments<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.comments.as_ref();
    }
    fn examples<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::Example>> {
        return self.examples.as_ref();
    }
    fn in_subset<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.in_subset.as_ref();
    }
    fn from_schema<'a>(&'a self) -> Option<&'a crate::uri> {
        return self.from_schema.as_ref();
    }
    fn imported_from<'a>(&'a self) -> Option<&'a str> {
        return self.imported_from.as_deref();
    }
    fn source<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.source.as_ref();
    }
    fn in_language<'a>(&'a self) -> Option<&'a str> {
        return self.in_language.as_deref();
    }
    fn see_also<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.see_also.as_ref();
    }
    fn deprecated_element_has_exact_replacement<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.deprecated_element_has_exact_replacement.as_ref();
    }
    fn deprecated_element_has_possible_replacement<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.deprecated_element_has_possible_replacement.as_ref();
    }
    fn aliases<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.aliases.as_ref();
    }
    fn structured_aliases<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::StructuredAlias>> {
        return self.structured_aliases.as_ref();
    }
    fn mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.mappings.as_ref();
    }
    fn exact_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.exact_mappings.as_ref();
    }
    fn close_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.close_mappings.as_ref();
    }
    fn related_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.related_mappings.as_ref();
    }
    fn narrow_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.narrow_mappings.as_ref();
    }
    fn broad_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.broad_mappings.as_ref();
    }
    fn created_by<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.created_by.as_ref();
    }
    fn contributors<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.contributors.as_ref();
    }
    fn created_on<'a>(&'a self) -> Option<&'a crate::NaiveDateTime> {
        return self.created_on.as_ref();
    }
    fn last_updated_on<'a>(&'a self) -> Option<&'a crate::NaiveDateTime> {
        return self.last_updated_on.as_ref();
    }
    fn modified_by<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.modified_by.as_ref();
    }
    fn status<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.status.as_ref();
    }
    fn rank(&self) -> Option<isize> {
        return self.rank;
    }
    fn categories<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.categories.as_ref();
    }
    fn keywords<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.keywords.as_ref();
    }
}
impl CommonMetadata for crate::SubsetDefinition {
    fn description<'a>(&'a self) -> Option<&'a str> {
        return self.description.as_deref();
    }
    fn alt_descriptions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::AltDescription>> {
        return self.alt_descriptions.as_ref();
    }
    fn title<'a>(&'a self) -> Option<&'a str> {
        return self.title.as_deref();
    }
    fn deprecated<'a>(&'a self) -> Option<&'a str> {
        return self.deprecated.as_deref();
    }
    fn todos<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.todos.as_ref();
    }
    fn notes<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.notes.as_ref();
    }
    fn comments<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.comments.as_ref();
    }
    fn examples<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::Example>> {
        return self.examples.as_ref();
    }
    fn in_subset<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.in_subset.as_ref();
    }
    fn from_schema<'a>(&'a self) -> Option<&'a crate::uri> {
        return self.from_schema.as_ref();
    }
    fn imported_from<'a>(&'a self) -> Option<&'a str> {
        return self.imported_from.as_deref();
    }
    fn source<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.source.as_ref();
    }
    fn in_language<'a>(&'a self) -> Option<&'a str> {
        return self.in_language.as_deref();
    }
    fn see_also<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.see_also.as_ref();
    }
    fn deprecated_element_has_exact_replacement<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.deprecated_element_has_exact_replacement.as_ref();
    }
    fn deprecated_element_has_possible_replacement<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.deprecated_element_has_possible_replacement.as_ref();
    }
    fn aliases<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.aliases.as_ref();
    }
    fn structured_aliases<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::StructuredAlias>> {
        return self
            .structured_aliases
            .as_ref()
            .map(|x| poly_containers::ListView::new(x));
    }
    fn mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.mappings.as_ref();
    }
    fn exact_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.exact_mappings.as_ref();
    }
    fn close_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.close_mappings.as_ref();
    }
    fn related_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.related_mappings.as_ref();
    }
    fn narrow_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.narrow_mappings.as_ref();
    }
    fn broad_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.broad_mappings.as_ref();
    }
    fn created_by<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.created_by.as_ref();
    }
    fn contributors<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.contributors.as_ref();
    }
    fn created_on<'a>(&'a self) -> Option<&'a crate::NaiveDateTime> {
        return self.created_on.as_ref();
    }
    fn last_updated_on<'a>(&'a self) -> Option<&'a crate::NaiveDateTime> {
        return self.last_updated_on.as_ref();
    }
    fn modified_by<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.modified_by.as_ref();
    }
    fn status<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.status.as_ref();
    }
    fn rank(&self) -> Option<isize> {
        return self.rank;
    }
    fn categories<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.categories.as_ref();
    }
    fn keywords<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.keywords.as_ref();
    }
}
impl CommonMetadata for crate::Definition {
    fn description<'a>(&'a self) -> Option<&'a str> {
        return self.description.as_deref();
    }
    fn alt_descriptions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::AltDescription>> {
        return self.alt_descriptions.as_ref();
    }
    fn title<'a>(&'a self) -> Option<&'a str> {
        return self.title.as_deref();
    }
    fn deprecated<'a>(&'a self) -> Option<&'a str> {
        return self.deprecated.as_deref();
    }
    fn todos<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.todos.as_ref();
    }
    fn notes<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.notes.as_ref();
    }
    fn comments<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.comments.as_ref();
    }
    fn examples<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::Example>> {
        return self.examples.as_ref();
    }
    fn in_subset<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.in_subset.as_ref();
    }
    fn from_schema<'a>(&'a self) -> Option<&'a crate::uri> {
        return self.from_schema.as_ref();
    }
    fn imported_from<'a>(&'a self) -> Option<&'a str> {
        return self.imported_from.as_deref();
    }
    fn source<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.source.as_ref();
    }
    fn in_language<'a>(&'a self) -> Option<&'a str> {
        return self.in_language.as_deref();
    }
    fn see_also<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.see_also.as_ref();
    }
    fn deprecated_element_has_exact_replacement<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.deprecated_element_has_exact_replacement.as_ref();
    }
    fn deprecated_element_has_possible_replacement<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.deprecated_element_has_possible_replacement.as_ref();
    }
    fn aliases<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.aliases.as_ref();
    }
    fn structured_aliases<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::StructuredAlias>> {
        return self.structured_aliases.as_ref();
    }
    fn mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.mappings.as_ref();
    }
    fn exact_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.exact_mappings.as_ref();
    }
    fn close_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.close_mappings.as_ref();
    }
    fn related_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.related_mappings.as_ref();
    }
    fn narrow_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.narrow_mappings.as_ref();
    }
    fn broad_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.broad_mappings.as_ref();
    }
    fn created_by<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.created_by.as_ref();
    }
    fn contributors<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.contributors.as_ref();
    }
    fn created_on<'a>(&'a self) -> Option<&'a crate::NaiveDateTime> {
        return self.created_on.as_ref();
    }
    fn last_updated_on<'a>(&'a self) -> Option<&'a crate::NaiveDateTime> {
        return self.last_updated_on.as_ref();
    }
    fn modified_by<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.modified_by.as_ref();
    }
    fn status<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.status.as_ref();
    }
    fn rank(&self) -> Option<isize> {
        return self.rank;
    }
    fn categories<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.categories.as_ref();
    }
    fn keywords<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.keywords.as_ref();
    }
}
impl CommonMetadata for crate::EnumDefinition {
    fn description<'a>(&'a self) -> Option<&'a str> {
        return self.description.as_deref();
    }
    fn alt_descriptions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::AltDescription>> {
        return self.alt_descriptions.as_ref();
    }
    fn title<'a>(&'a self) -> Option<&'a str> {
        return self.title.as_deref();
    }
    fn deprecated<'a>(&'a self) -> Option<&'a str> {
        return self.deprecated.as_deref();
    }
    fn todos<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.todos.as_ref();
    }
    fn notes<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.notes.as_ref();
    }
    fn comments<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.comments.as_ref();
    }
    fn examples<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::Example>> {
        return self.examples.as_ref();
    }
    fn in_subset<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.in_subset.as_ref();
    }
    fn from_schema<'a>(&'a self) -> Option<&'a crate::uri> {
        return self.from_schema.as_ref();
    }
    fn imported_from<'a>(&'a self) -> Option<&'a str> {
        return self.imported_from.as_deref();
    }
    fn source<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.source.as_ref();
    }
    fn in_language<'a>(&'a self) -> Option<&'a str> {
        return self.in_language.as_deref();
    }
    fn see_also<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.see_also.as_ref();
    }
    fn deprecated_element_has_exact_replacement<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.deprecated_element_has_exact_replacement.as_ref();
    }
    fn deprecated_element_has_possible_replacement<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.deprecated_element_has_possible_replacement.as_ref();
    }
    fn aliases<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.aliases.as_ref();
    }
    fn structured_aliases<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::StructuredAlias>> {
        return self.structured_aliases.as_ref();
    }
    fn mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.mappings.as_ref();
    }
    fn exact_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.exact_mappings.as_ref();
    }
    fn close_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.close_mappings.as_ref();
    }
    fn related_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.related_mappings.as_ref();
    }
    fn narrow_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.narrow_mappings.as_ref();
    }
    fn broad_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.broad_mappings.as_ref();
    }
    fn created_by<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.created_by.as_ref();
    }
    fn contributors<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.contributors.as_ref();
    }
    fn created_on<'a>(&'a self) -> Option<&'a crate::NaiveDateTime> {
        return self.created_on.as_ref();
    }
    fn last_updated_on<'a>(&'a self) -> Option<&'a crate::NaiveDateTime> {
        return self.last_updated_on.as_ref();
    }
    fn modified_by<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.modified_by.as_ref();
    }
    fn status<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.status.as_ref();
    }
    fn rank(&self) -> Option<isize> {
        return self.rank;
    }
    fn categories<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.categories.as_ref();
    }
    fn keywords<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.keywords.as_ref();
    }
}
impl CommonMetadata for crate::SlotDefinition {
    fn description<'a>(&'a self) -> Option<&'a str> {
        return self.description.as_deref();
    }
    fn alt_descriptions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::AltDescription>> {
        return self.alt_descriptions.as_ref();
    }
    fn title<'a>(&'a self) -> Option<&'a str> {
        return self.title.as_deref();
    }
    fn deprecated<'a>(&'a self) -> Option<&'a str> {
        return self.deprecated.as_deref();
    }
    fn todos<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.todos.as_ref();
    }
    fn notes<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.notes.as_ref();
    }
    fn comments<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.comments.as_ref();
    }
    fn examples<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::Example>> {
        return self.examples.as_ref();
    }
    fn in_subset<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.in_subset.as_ref();
    }
    fn from_schema<'a>(&'a self) -> Option<&'a crate::uri> {
        return self.from_schema.as_ref();
    }
    fn imported_from<'a>(&'a self) -> Option<&'a str> {
        return self.imported_from.as_deref();
    }
    fn source<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.source.as_ref();
    }
    fn in_language<'a>(&'a self) -> Option<&'a str> {
        return self.in_language.as_deref();
    }
    fn see_also<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.see_also.as_ref();
    }
    fn deprecated_element_has_exact_replacement<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.deprecated_element_has_exact_replacement.as_ref();
    }
    fn deprecated_element_has_possible_replacement<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.deprecated_element_has_possible_replacement.as_ref();
    }
    fn aliases<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.aliases.as_ref();
    }
    fn structured_aliases<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::StructuredAlias>> {
        return self.structured_aliases.as_ref();
    }
    fn mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.mappings.as_ref();
    }
    fn exact_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.exact_mappings.as_ref();
    }
    fn close_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.close_mappings.as_ref();
    }
    fn related_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.related_mappings.as_ref();
    }
    fn narrow_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.narrow_mappings.as_ref();
    }
    fn broad_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.broad_mappings.as_ref();
    }
    fn created_by<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.created_by.as_ref();
    }
    fn contributors<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.contributors.as_ref();
    }
    fn created_on<'a>(&'a self) -> Option<&'a crate::NaiveDateTime> {
        return self.created_on.as_ref();
    }
    fn last_updated_on<'a>(&'a self) -> Option<&'a crate::NaiveDateTime> {
        return self.last_updated_on.as_ref();
    }
    fn modified_by<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.modified_by.as_ref();
    }
    fn status<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.status.as_ref();
    }
    fn rank(&self) -> Option<isize> {
        return self.rank;
    }
    fn categories<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.categories.as_ref();
    }
    fn keywords<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.keywords.as_ref();
    }
}
impl CommonMetadata for crate::ClassDefinition {
    fn description<'a>(&'a self) -> Option<&'a str> {
        return self.description.as_deref();
    }
    fn alt_descriptions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::AltDescription>> {
        return self.alt_descriptions.as_ref();
    }
    fn title<'a>(&'a self) -> Option<&'a str> {
        return self.title.as_deref();
    }
    fn deprecated<'a>(&'a self) -> Option<&'a str> {
        return self.deprecated.as_deref();
    }
    fn todos<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.todos.as_ref();
    }
    fn notes<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.notes.as_ref();
    }
    fn comments<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.comments.as_ref();
    }
    fn examples<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::Example>> {
        return self.examples.as_ref();
    }
    fn in_subset<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.in_subset.as_ref();
    }
    fn from_schema<'a>(&'a self) -> Option<&'a crate::uri> {
        return self.from_schema.as_ref();
    }
    fn imported_from<'a>(&'a self) -> Option<&'a str> {
        return self.imported_from.as_deref();
    }
    fn source<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.source.as_ref();
    }
    fn in_language<'a>(&'a self) -> Option<&'a str> {
        return self.in_language.as_deref();
    }
    fn see_also<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.see_also.as_ref();
    }
    fn deprecated_element_has_exact_replacement<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.deprecated_element_has_exact_replacement.as_ref();
    }
    fn deprecated_element_has_possible_replacement<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.deprecated_element_has_possible_replacement.as_ref();
    }
    fn aliases<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.aliases.as_ref();
    }
    fn structured_aliases<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::StructuredAlias>> {
        return self.structured_aliases.as_ref();
    }
    fn mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.mappings.as_ref();
    }
    fn exact_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.exact_mappings.as_ref();
    }
    fn close_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.close_mappings.as_ref();
    }
    fn related_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.related_mappings.as_ref();
    }
    fn narrow_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.narrow_mappings.as_ref();
    }
    fn broad_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.broad_mappings.as_ref();
    }
    fn created_by<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.created_by.as_ref();
    }
    fn contributors<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.contributors.as_ref();
    }
    fn created_on<'a>(&'a self) -> Option<&'a crate::NaiveDateTime> {
        return self.created_on.as_ref();
    }
    fn last_updated_on<'a>(&'a self) -> Option<&'a crate::NaiveDateTime> {
        return self.last_updated_on.as_ref();
    }
    fn modified_by<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.modified_by.as_ref();
    }
    fn status<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.status.as_ref();
    }
    fn rank(&self) -> Option<isize> {
        return self.rank;
    }
    fn categories<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.categories.as_ref();
    }
    fn keywords<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.keywords.as_ref();
    }
}

impl CommonMetadata for crate::CommonMetadataOrSubtype {
    fn description<'a>(&'a self) -> Option<&'a str> {
        match self {
            CommonMetadataOrSubtype::Element(val) => val.description(),
            CommonMetadataOrSubtype::EnumBinding(val) => val.description(),
            CommonMetadataOrSubtype::StructuredAlias(val) => val.description(),
            CommonMetadataOrSubtype::AnonymousExpression(val) => val.description(),
            CommonMetadataOrSubtype::PathExpression(val) => val.description(),
            CommonMetadataOrSubtype::ClassRule(val) => val.description(),
            CommonMetadataOrSubtype::ArrayExpression(val) => val.description(),
            CommonMetadataOrSubtype::DimensionExpression(val) => val.description(),
            CommonMetadataOrSubtype::PatternExpression(val) => val.description(),
            CommonMetadataOrSubtype::ImportExpression(val) => val.description(),
            CommonMetadataOrSubtype::PermissibleValue(val) => val.description(),
            CommonMetadataOrSubtype::UniqueKey(val) => val.description(),
            CommonMetadataOrSubtype::TypeMapping(val) => val.description(),
            CommonMetadataOrSubtype::AnonymousSlotExpression(val) => val.description(),
            CommonMetadataOrSubtype::AnonymousClassExpression(val) => val.description(),
            CommonMetadataOrSubtype::SchemaDefinition(val) => val.description(),
            CommonMetadataOrSubtype::TypeDefinition(val) => val.description(),
            CommonMetadataOrSubtype::SubsetDefinition(val) => val.description(),
            CommonMetadataOrSubtype::Definition(val) => val.description(),
            CommonMetadataOrSubtype::EnumDefinition(val) => val.description(),
            CommonMetadataOrSubtype::SlotDefinition(val) => val.description(),
            CommonMetadataOrSubtype::ClassDefinition(val) => val.description(),
        }
    }
    fn alt_descriptions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::AltDescription>> {
        match self {
            CommonMetadataOrSubtype::Element(val) => val.alt_descriptions().map(|x| x.to_any()),
            CommonMetadataOrSubtype::EnumBinding(val) => val.alt_descriptions().map(|x| x.to_any()),
            CommonMetadataOrSubtype::StructuredAlias(val) => {
                val.alt_descriptions().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::AnonymousExpression(val) => {
                val.alt_descriptions().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::PathExpression(val) => {
                val.alt_descriptions().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::ClassRule(val) => val.alt_descriptions().map(|x| x.to_any()),
            CommonMetadataOrSubtype::ArrayExpression(val) => {
                val.alt_descriptions().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::DimensionExpression(val) => {
                val.alt_descriptions().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::PatternExpression(val) => {
                val.alt_descriptions().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::ImportExpression(val) => {
                val.alt_descriptions().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::PermissibleValue(val) => {
                val.alt_descriptions().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::UniqueKey(val) => val.alt_descriptions().map(|x| x.to_any()),
            CommonMetadataOrSubtype::TypeMapping(val) => val.alt_descriptions().map(|x| x.to_any()),
            CommonMetadataOrSubtype::AnonymousSlotExpression(val) => {
                val.alt_descriptions().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::AnonymousClassExpression(val) => {
                val.alt_descriptions().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::SchemaDefinition(val) => {
                val.alt_descriptions().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::TypeDefinition(val) => {
                val.alt_descriptions().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::SubsetDefinition(val) => {
                val.alt_descriptions().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::Definition(val) => val.alt_descriptions().map(|x| x.to_any()),
            CommonMetadataOrSubtype::EnumDefinition(val) => {
                val.alt_descriptions().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::SlotDefinition(val) => {
                val.alt_descriptions().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::ClassDefinition(val) => {
                val.alt_descriptions().map(|x| x.to_any())
            }
        }
    }
    fn title<'a>(&'a self) -> Option<&'a str> {
        match self {
            CommonMetadataOrSubtype::Element(val) => val.title(),
            CommonMetadataOrSubtype::EnumBinding(val) => val.title(),
            CommonMetadataOrSubtype::StructuredAlias(val) => val.title(),
            CommonMetadataOrSubtype::AnonymousExpression(val) => val.title(),
            CommonMetadataOrSubtype::PathExpression(val) => val.title(),
            CommonMetadataOrSubtype::ClassRule(val) => val.title(),
            CommonMetadataOrSubtype::ArrayExpression(val) => val.title(),
            CommonMetadataOrSubtype::DimensionExpression(val) => val.title(),
            CommonMetadataOrSubtype::PatternExpression(val) => val.title(),
            CommonMetadataOrSubtype::ImportExpression(val) => val.title(),
            CommonMetadataOrSubtype::PermissibleValue(val) => val.title(),
            CommonMetadataOrSubtype::UniqueKey(val) => val.title(),
            CommonMetadataOrSubtype::TypeMapping(val) => val.title(),
            CommonMetadataOrSubtype::AnonymousSlotExpression(val) => val.title(),
            CommonMetadataOrSubtype::AnonymousClassExpression(val) => val.title(),
            CommonMetadataOrSubtype::SchemaDefinition(val) => val.title(),
            CommonMetadataOrSubtype::TypeDefinition(val) => val.title(),
            CommonMetadataOrSubtype::SubsetDefinition(val) => val.title(),
            CommonMetadataOrSubtype::Definition(val) => val.title(),
            CommonMetadataOrSubtype::EnumDefinition(val) => val.title(),
            CommonMetadataOrSubtype::SlotDefinition(val) => val.title(),
            CommonMetadataOrSubtype::ClassDefinition(val) => val.title(),
        }
    }
    fn deprecated<'a>(&'a self) -> Option<&'a str> {
        match self {
            CommonMetadataOrSubtype::Element(val) => val.deprecated(),
            CommonMetadataOrSubtype::EnumBinding(val) => val.deprecated(),
            CommonMetadataOrSubtype::StructuredAlias(val) => val.deprecated(),
            CommonMetadataOrSubtype::AnonymousExpression(val) => val.deprecated(),
            CommonMetadataOrSubtype::PathExpression(val) => val.deprecated(),
            CommonMetadataOrSubtype::ClassRule(val) => val.deprecated(),
            CommonMetadataOrSubtype::ArrayExpression(val) => val.deprecated(),
            CommonMetadataOrSubtype::DimensionExpression(val) => val.deprecated(),
            CommonMetadataOrSubtype::PatternExpression(val) => val.deprecated(),
            CommonMetadataOrSubtype::ImportExpression(val) => val.deprecated(),
            CommonMetadataOrSubtype::PermissibleValue(val) => val.deprecated(),
            CommonMetadataOrSubtype::UniqueKey(val) => val.deprecated(),
            CommonMetadataOrSubtype::TypeMapping(val) => val.deprecated(),
            CommonMetadataOrSubtype::AnonymousSlotExpression(val) => val.deprecated(),
            CommonMetadataOrSubtype::AnonymousClassExpression(val) => val.deprecated(),
            CommonMetadataOrSubtype::SchemaDefinition(val) => val.deprecated(),
            CommonMetadataOrSubtype::TypeDefinition(val) => val.deprecated(),
            CommonMetadataOrSubtype::SubsetDefinition(val) => val.deprecated(),
            CommonMetadataOrSubtype::Definition(val) => val.deprecated(),
            CommonMetadataOrSubtype::EnumDefinition(val) => val.deprecated(),
            CommonMetadataOrSubtype::SlotDefinition(val) => val.deprecated(),
            CommonMetadataOrSubtype::ClassDefinition(val) => val.deprecated(),
        }
    }
    fn todos<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        match self {
            CommonMetadataOrSubtype::Element(val) => val.todos().map(|x| x.to_any()),
            CommonMetadataOrSubtype::EnumBinding(val) => val.todos().map(|x| x.to_any()),
            CommonMetadataOrSubtype::StructuredAlias(val) => val.todos().map(|x| x.to_any()),
            CommonMetadataOrSubtype::AnonymousExpression(val) => val.todos().map(|x| x.to_any()),
            CommonMetadataOrSubtype::PathExpression(val) => val.todos().map(|x| x.to_any()),
            CommonMetadataOrSubtype::ClassRule(val) => val.todos().map(|x| x.to_any()),
            CommonMetadataOrSubtype::ArrayExpression(val) => val.todos().map(|x| x.to_any()),
            CommonMetadataOrSubtype::DimensionExpression(val) => val.todos().map(|x| x.to_any()),
            CommonMetadataOrSubtype::PatternExpression(val) => val.todos().map(|x| x.to_any()),
            CommonMetadataOrSubtype::ImportExpression(val) => val.todos().map(|x| x.to_any()),
            CommonMetadataOrSubtype::PermissibleValue(val) => val.todos().map(|x| x.to_any()),
            CommonMetadataOrSubtype::UniqueKey(val) => val.todos().map(|x| x.to_any()),
            CommonMetadataOrSubtype::TypeMapping(val) => val.todos().map(|x| x.to_any()),
            CommonMetadataOrSubtype::AnonymousSlotExpression(val) => {
                val.todos().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::AnonymousClassExpression(val) => {
                val.todos().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::SchemaDefinition(val) => val.todos().map(|x| x.to_any()),
            CommonMetadataOrSubtype::TypeDefinition(val) => val.todos().map(|x| x.to_any()),
            CommonMetadataOrSubtype::SubsetDefinition(val) => val.todos().map(|x| x.to_any()),
            CommonMetadataOrSubtype::Definition(val) => val.todos().map(|x| x.to_any()),
            CommonMetadataOrSubtype::EnumDefinition(val) => val.todos().map(|x| x.to_any()),
            CommonMetadataOrSubtype::SlotDefinition(val) => val.todos().map(|x| x.to_any()),
            CommonMetadataOrSubtype::ClassDefinition(val) => val.todos().map(|x| x.to_any()),
        }
    }
    fn notes<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        match self {
            CommonMetadataOrSubtype::Element(val) => val.notes().map(|x| x.to_any()),
            CommonMetadataOrSubtype::EnumBinding(val) => val.notes().map(|x| x.to_any()),
            CommonMetadataOrSubtype::StructuredAlias(val) => val.notes().map(|x| x.to_any()),
            CommonMetadataOrSubtype::AnonymousExpression(val) => val.notes().map(|x| x.to_any()),
            CommonMetadataOrSubtype::PathExpression(val) => val.notes().map(|x| x.to_any()),
            CommonMetadataOrSubtype::ClassRule(val) => val.notes().map(|x| x.to_any()),
            CommonMetadataOrSubtype::ArrayExpression(val) => val.notes().map(|x| x.to_any()),
            CommonMetadataOrSubtype::DimensionExpression(val) => val.notes().map(|x| x.to_any()),
            CommonMetadataOrSubtype::PatternExpression(val) => val.notes().map(|x| x.to_any()),
            CommonMetadataOrSubtype::ImportExpression(val) => val.notes().map(|x| x.to_any()),
            CommonMetadataOrSubtype::PermissibleValue(val) => val.notes().map(|x| x.to_any()),
            CommonMetadataOrSubtype::UniqueKey(val) => val.notes().map(|x| x.to_any()),
            CommonMetadataOrSubtype::TypeMapping(val) => val.notes().map(|x| x.to_any()),
            CommonMetadataOrSubtype::AnonymousSlotExpression(val) => {
                val.notes().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::AnonymousClassExpression(val) => {
                val.notes().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::SchemaDefinition(val) => val.notes().map(|x| x.to_any()),
            CommonMetadataOrSubtype::TypeDefinition(val) => val.notes().map(|x| x.to_any()),
            CommonMetadataOrSubtype::SubsetDefinition(val) => val.notes().map(|x| x.to_any()),
            CommonMetadataOrSubtype::Definition(val) => val.notes().map(|x| x.to_any()),
            CommonMetadataOrSubtype::EnumDefinition(val) => val.notes().map(|x| x.to_any()),
            CommonMetadataOrSubtype::SlotDefinition(val) => val.notes().map(|x| x.to_any()),
            CommonMetadataOrSubtype::ClassDefinition(val) => val.notes().map(|x| x.to_any()),
        }
    }
    fn comments<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        match self {
            CommonMetadataOrSubtype::Element(val) => val.comments().map(|x| x.to_any()),
            CommonMetadataOrSubtype::EnumBinding(val) => val.comments().map(|x| x.to_any()),
            CommonMetadataOrSubtype::StructuredAlias(val) => val.comments().map(|x| x.to_any()),
            CommonMetadataOrSubtype::AnonymousExpression(val) => val.comments().map(|x| x.to_any()),
            CommonMetadataOrSubtype::PathExpression(val) => val.comments().map(|x| x.to_any()),
            CommonMetadataOrSubtype::ClassRule(val) => val.comments().map(|x| x.to_any()),
            CommonMetadataOrSubtype::ArrayExpression(val) => val.comments().map(|x| x.to_any()),
            CommonMetadataOrSubtype::DimensionExpression(val) => val.comments().map(|x| x.to_any()),
            CommonMetadataOrSubtype::PatternExpression(val) => val.comments().map(|x| x.to_any()),
            CommonMetadataOrSubtype::ImportExpression(val) => val.comments().map(|x| x.to_any()),
            CommonMetadataOrSubtype::PermissibleValue(val) => val.comments().map(|x| x.to_any()),
            CommonMetadataOrSubtype::UniqueKey(val) => val.comments().map(|x| x.to_any()),
            CommonMetadataOrSubtype::TypeMapping(val) => val.comments().map(|x| x.to_any()),
            CommonMetadataOrSubtype::AnonymousSlotExpression(val) => {
                val.comments().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::AnonymousClassExpression(val) => {
                val.comments().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::SchemaDefinition(val) => val.comments().map(|x| x.to_any()),
            CommonMetadataOrSubtype::TypeDefinition(val) => val.comments().map(|x| x.to_any()),
            CommonMetadataOrSubtype::SubsetDefinition(val) => val.comments().map(|x| x.to_any()),
            CommonMetadataOrSubtype::Definition(val) => val.comments().map(|x| x.to_any()),
            CommonMetadataOrSubtype::EnumDefinition(val) => val.comments().map(|x| x.to_any()),
            CommonMetadataOrSubtype::SlotDefinition(val) => val.comments().map(|x| x.to_any()),
            CommonMetadataOrSubtype::ClassDefinition(val) => val.comments().map(|x| x.to_any()),
        }
    }
    fn examples<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::Example>> {
        match self {
            CommonMetadataOrSubtype::Element(val) => val.examples().map(|x| x.to_any()),
            CommonMetadataOrSubtype::EnumBinding(val) => val.examples().map(|x| x.to_any()),
            CommonMetadataOrSubtype::StructuredAlias(val) => val.examples().map(|x| x.to_any()),
            CommonMetadataOrSubtype::AnonymousExpression(val) => val.examples().map(|x| x.to_any()),
            CommonMetadataOrSubtype::PathExpression(val) => val.examples().map(|x| x.to_any()),
            CommonMetadataOrSubtype::ClassRule(val) => val.examples().map(|x| x.to_any()),
            CommonMetadataOrSubtype::ArrayExpression(val) => val.examples().map(|x| x.to_any()),
            CommonMetadataOrSubtype::DimensionExpression(val) => val.examples().map(|x| x.to_any()),
            CommonMetadataOrSubtype::PatternExpression(val) => val.examples().map(|x| x.to_any()),
            CommonMetadataOrSubtype::ImportExpression(val) => val.examples().map(|x| x.to_any()),
            CommonMetadataOrSubtype::PermissibleValue(val) => val.examples().map(|x| x.to_any()),
            CommonMetadataOrSubtype::UniqueKey(val) => val.examples().map(|x| x.to_any()),
            CommonMetadataOrSubtype::TypeMapping(val) => val.examples().map(|x| x.to_any()),
            CommonMetadataOrSubtype::AnonymousSlotExpression(val) => {
                val.examples().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::AnonymousClassExpression(val) => {
                val.examples().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::SchemaDefinition(val) => val.examples().map(|x| x.to_any()),
            CommonMetadataOrSubtype::TypeDefinition(val) => val.examples().map(|x| x.to_any()),
            CommonMetadataOrSubtype::SubsetDefinition(val) => val.examples().map(|x| x.to_any()),
            CommonMetadataOrSubtype::Definition(val) => val.examples().map(|x| x.to_any()),
            CommonMetadataOrSubtype::EnumDefinition(val) => val.examples().map(|x| x.to_any()),
            CommonMetadataOrSubtype::SlotDefinition(val) => val.examples().map(|x| x.to_any()),
            CommonMetadataOrSubtype::ClassDefinition(val) => val.examples().map(|x| x.to_any()),
        }
    }
    fn in_subset<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        match self {
            CommonMetadataOrSubtype::Element(val) => val.in_subset().map(|x| x.to_any()),
            CommonMetadataOrSubtype::EnumBinding(val) => val.in_subset().map(|x| x.to_any()),
            CommonMetadataOrSubtype::StructuredAlias(val) => val.in_subset().map(|x| x.to_any()),
            CommonMetadataOrSubtype::AnonymousExpression(val) => {
                val.in_subset().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::PathExpression(val) => val.in_subset().map(|x| x.to_any()),
            CommonMetadataOrSubtype::ClassRule(val) => val.in_subset().map(|x| x.to_any()),
            CommonMetadataOrSubtype::ArrayExpression(val) => val.in_subset().map(|x| x.to_any()),
            CommonMetadataOrSubtype::DimensionExpression(val) => {
                val.in_subset().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::PatternExpression(val) => val.in_subset().map(|x| x.to_any()),
            CommonMetadataOrSubtype::ImportExpression(val) => val.in_subset().map(|x| x.to_any()),
            CommonMetadataOrSubtype::PermissibleValue(val) => val.in_subset().map(|x| x.to_any()),
            CommonMetadataOrSubtype::UniqueKey(val) => val.in_subset().map(|x| x.to_any()),
            CommonMetadataOrSubtype::TypeMapping(val) => val.in_subset().map(|x| x.to_any()),
            CommonMetadataOrSubtype::AnonymousSlotExpression(val) => {
                val.in_subset().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::AnonymousClassExpression(val) => {
                val.in_subset().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::SchemaDefinition(val) => val.in_subset().map(|x| x.to_any()),
            CommonMetadataOrSubtype::TypeDefinition(val) => val.in_subset().map(|x| x.to_any()),
            CommonMetadataOrSubtype::SubsetDefinition(val) => val.in_subset().map(|x| x.to_any()),
            CommonMetadataOrSubtype::Definition(val) => val.in_subset().map(|x| x.to_any()),
            CommonMetadataOrSubtype::EnumDefinition(val) => val.in_subset().map(|x| x.to_any()),
            CommonMetadataOrSubtype::SlotDefinition(val) => val.in_subset().map(|x| x.to_any()),
            CommonMetadataOrSubtype::ClassDefinition(val) => val.in_subset().map(|x| x.to_any()),
        }
    }
    fn from_schema<'a>(&'a self) -> Option<&'a crate::uri> {
        match self {
            CommonMetadataOrSubtype::Element(val) => val.from_schema(),
            CommonMetadataOrSubtype::EnumBinding(val) => val.from_schema(),
            CommonMetadataOrSubtype::StructuredAlias(val) => val.from_schema(),
            CommonMetadataOrSubtype::AnonymousExpression(val) => val.from_schema(),
            CommonMetadataOrSubtype::PathExpression(val) => val.from_schema(),
            CommonMetadataOrSubtype::ClassRule(val) => val.from_schema(),
            CommonMetadataOrSubtype::ArrayExpression(val) => val.from_schema(),
            CommonMetadataOrSubtype::DimensionExpression(val) => val.from_schema(),
            CommonMetadataOrSubtype::PatternExpression(val) => val.from_schema(),
            CommonMetadataOrSubtype::ImportExpression(val) => val.from_schema(),
            CommonMetadataOrSubtype::PermissibleValue(val) => val.from_schema(),
            CommonMetadataOrSubtype::UniqueKey(val) => val.from_schema(),
            CommonMetadataOrSubtype::TypeMapping(val) => val.from_schema(),
            CommonMetadataOrSubtype::AnonymousSlotExpression(val) => val.from_schema(),
            CommonMetadataOrSubtype::AnonymousClassExpression(val) => val.from_schema(),
            CommonMetadataOrSubtype::SchemaDefinition(val) => val.from_schema(),
            CommonMetadataOrSubtype::TypeDefinition(val) => val.from_schema(),
            CommonMetadataOrSubtype::SubsetDefinition(val) => val.from_schema(),
            CommonMetadataOrSubtype::Definition(val) => val.from_schema(),
            CommonMetadataOrSubtype::EnumDefinition(val) => val.from_schema(),
            CommonMetadataOrSubtype::SlotDefinition(val) => val.from_schema(),
            CommonMetadataOrSubtype::ClassDefinition(val) => val.from_schema(),
        }
    }
    fn imported_from<'a>(&'a self) -> Option<&'a str> {
        match self {
            CommonMetadataOrSubtype::Element(val) => val.imported_from(),
            CommonMetadataOrSubtype::EnumBinding(val) => val.imported_from(),
            CommonMetadataOrSubtype::StructuredAlias(val) => val.imported_from(),
            CommonMetadataOrSubtype::AnonymousExpression(val) => val.imported_from(),
            CommonMetadataOrSubtype::PathExpression(val) => val.imported_from(),
            CommonMetadataOrSubtype::ClassRule(val) => val.imported_from(),
            CommonMetadataOrSubtype::ArrayExpression(val) => val.imported_from(),
            CommonMetadataOrSubtype::DimensionExpression(val) => val.imported_from(),
            CommonMetadataOrSubtype::PatternExpression(val) => val.imported_from(),
            CommonMetadataOrSubtype::ImportExpression(val) => val.imported_from(),
            CommonMetadataOrSubtype::PermissibleValue(val) => val.imported_from(),
            CommonMetadataOrSubtype::UniqueKey(val) => val.imported_from(),
            CommonMetadataOrSubtype::TypeMapping(val) => val.imported_from(),
            CommonMetadataOrSubtype::AnonymousSlotExpression(val) => val.imported_from(),
            CommonMetadataOrSubtype::AnonymousClassExpression(val) => val.imported_from(),
            CommonMetadataOrSubtype::SchemaDefinition(val) => val.imported_from(),
            CommonMetadataOrSubtype::TypeDefinition(val) => val.imported_from(),
            CommonMetadataOrSubtype::SubsetDefinition(val) => val.imported_from(),
            CommonMetadataOrSubtype::Definition(val) => val.imported_from(),
            CommonMetadataOrSubtype::EnumDefinition(val) => val.imported_from(),
            CommonMetadataOrSubtype::SlotDefinition(val) => val.imported_from(),
            CommonMetadataOrSubtype::ClassDefinition(val) => val.imported_from(),
        }
    }
    fn source<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        match self {
            CommonMetadataOrSubtype::Element(val) => val.source(),
            CommonMetadataOrSubtype::EnumBinding(val) => val.source(),
            CommonMetadataOrSubtype::StructuredAlias(val) => val.source(),
            CommonMetadataOrSubtype::AnonymousExpression(val) => val.source(),
            CommonMetadataOrSubtype::PathExpression(val) => val.source(),
            CommonMetadataOrSubtype::ClassRule(val) => val.source(),
            CommonMetadataOrSubtype::ArrayExpression(val) => val.source(),
            CommonMetadataOrSubtype::DimensionExpression(val) => val.source(),
            CommonMetadataOrSubtype::PatternExpression(val) => val.source(),
            CommonMetadataOrSubtype::ImportExpression(val) => val.source(),
            CommonMetadataOrSubtype::PermissibleValue(val) => val.source(),
            CommonMetadataOrSubtype::UniqueKey(val) => val.source(),
            CommonMetadataOrSubtype::TypeMapping(val) => val.source(),
            CommonMetadataOrSubtype::AnonymousSlotExpression(val) => val.source(),
            CommonMetadataOrSubtype::AnonymousClassExpression(val) => val.source(),
            CommonMetadataOrSubtype::SchemaDefinition(val) => val.source(),
            CommonMetadataOrSubtype::TypeDefinition(val) => val.source(),
            CommonMetadataOrSubtype::SubsetDefinition(val) => val.source(),
            CommonMetadataOrSubtype::Definition(val) => val.source(),
            CommonMetadataOrSubtype::EnumDefinition(val) => val.source(),
            CommonMetadataOrSubtype::SlotDefinition(val) => val.source(),
            CommonMetadataOrSubtype::ClassDefinition(val) => val.source(),
        }
    }
    fn in_language<'a>(&'a self) -> Option<&'a str> {
        match self {
            CommonMetadataOrSubtype::Element(val) => val.in_language(),
            CommonMetadataOrSubtype::EnumBinding(val) => val.in_language(),
            CommonMetadataOrSubtype::StructuredAlias(val) => val.in_language(),
            CommonMetadataOrSubtype::AnonymousExpression(val) => val.in_language(),
            CommonMetadataOrSubtype::PathExpression(val) => val.in_language(),
            CommonMetadataOrSubtype::ClassRule(val) => val.in_language(),
            CommonMetadataOrSubtype::ArrayExpression(val) => val.in_language(),
            CommonMetadataOrSubtype::DimensionExpression(val) => val.in_language(),
            CommonMetadataOrSubtype::PatternExpression(val) => val.in_language(),
            CommonMetadataOrSubtype::ImportExpression(val) => val.in_language(),
            CommonMetadataOrSubtype::PermissibleValue(val) => val.in_language(),
            CommonMetadataOrSubtype::UniqueKey(val) => val.in_language(),
            CommonMetadataOrSubtype::TypeMapping(val) => val.in_language(),
            CommonMetadataOrSubtype::AnonymousSlotExpression(val) => val.in_language(),
            CommonMetadataOrSubtype::AnonymousClassExpression(val) => val.in_language(),
            CommonMetadataOrSubtype::SchemaDefinition(val) => val.in_language(),
            CommonMetadataOrSubtype::TypeDefinition(val) => val.in_language(),
            CommonMetadataOrSubtype::SubsetDefinition(val) => val.in_language(),
            CommonMetadataOrSubtype::Definition(val) => val.in_language(),
            CommonMetadataOrSubtype::EnumDefinition(val) => val.in_language(),
            CommonMetadataOrSubtype::SlotDefinition(val) => val.in_language(),
            CommonMetadataOrSubtype::ClassDefinition(val) => val.in_language(),
        }
    }
    fn see_also<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        match self {
            CommonMetadataOrSubtype::Element(val) => val.see_also().map(|x| x.to_any()),
            CommonMetadataOrSubtype::EnumBinding(val) => val.see_also().map(|x| x.to_any()),
            CommonMetadataOrSubtype::StructuredAlias(val) => val.see_also().map(|x| x.to_any()),
            CommonMetadataOrSubtype::AnonymousExpression(val) => val.see_also().map(|x| x.to_any()),
            CommonMetadataOrSubtype::PathExpression(val) => val.see_also().map(|x| x.to_any()),
            CommonMetadataOrSubtype::ClassRule(val) => val.see_also().map(|x| x.to_any()),
            CommonMetadataOrSubtype::ArrayExpression(val) => val.see_also().map(|x| x.to_any()),
            CommonMetadataOrSubtype::DimensionExpression(val) => val.see_also().map(|x| x.to_any()),
            CommonMetadataOrSubtype::PatternExpression(val) => val.see_also().map(|x| x.to_any()),
            CommonMetadataOrSubtype::ImportExpression(val) => val.see_also().map(|x| x.to_any()),
            CommonMetadataOrSubtype::PermissibleValue(val) => val.see_also().map(|x| x.to_any()),
            CommonMetadataOrSubtype::UniqueKey(val) => val.see_also().map(|x| x.to_any()),
            CommonMetadataOrSubtype::TypeMapping(val) => val.see_also().map(|x| x.to_any()),
            CommonMetadataOrSubtype::AnonymousSlotExpression(val) => {
                val.see_also().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::AnonymousClassExpression(val) => {
                val.see_also().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::SchemaDefinition(val) => val.see_also().map(|x| x.to_any()),
            CommonMetadataOrSubtype::TypeDefinition(val) => val.see_also().map(|x| x.to_any()),
            CommonMetadataOrSubtype::SubsetDefinition(val) => val.see_also().map(|x| x.to_any()),
            CommonMetadataOrSubtype::Definition(val) => val.see_also().map(|x| x.to_any()),
            CommonMetadataOrSubtype::EnumDefinition(val) => val.see_also().map(|x| x.to_any()),
            CommonMetadataOrSubtype::SlotDefinition(val) => val.see_also().map(|x| x.to_any()),
            CommonMetadataOrSubtype::ClassDefinition(val) => val.see_also().map(|x| x.to_any()),
        }
    }
    fn deprecated_element_has_exact_replacement<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        match self {
            CommonMetadataOrSubtype::Element(val) => val.deprecated_element_has_exact_replacement(),
            CommonMetadataOrSubtype::EnumBinding(val) => {
                val.deprecated_element_has_exact_replacement()
            }
            CommonMetadataOrSubtype::StructuredAlias(val) => {
                val.deprecated_element_has_exact_replacement()
            }
            CommonMetadataOrSubtype::AnonymousExpression(val) => {
                val.deprecated_element_has_exact_replacement()
            }
            CommonMetadataOrSubtype::PathExpression(val) => {
                val.deprecated_element_has_exact_replacement()
            }
            CommonMetadataOrSubtype::ClassRule(val) => {
                val.deprecated_element_has_exact_replacement()
            }
            CommonMetadataOrSubtype::ArrayExpression(val) => {
                val.deprecated_element_has_exact_replacement()
            }
            CommonMetadataOrSubtype::DimensionExpression(val) => {
                val.deprecated_element_has_exact_replacement()
            }
            CommonMetadataOrSubtype::PatternExpression(val) => {
                val.deprecated_element_has_exact_replacement()
            }
            CommonMetadataOrSubtype::ImportExpression(val) => {
                val.deprecated_element_has_exact_replacement()
            }
            CommonMetadataOrSubtype::PermissibleValue(val) => {
                val.deprecated_element_has_exact_replacement()
            }
            CommonMetadataOrSubtype::UniqueKey(val) => {
                val.deprecated_element_has_exact_replacement()
            }
            CommonMetadataOrSubtype::TypeMapping(val) => {
                val.deprecated_element_has_exact_replacement()
            }
            CommonMetadataOrSubtype::AnonymousSlotExpression(val) => {
                val.deprecated_element_has_exact_replacement()
            }
            CommonMetadataOrSubtype::AnonymousClassExpression(val) => {
                val.deprecated_element_has_exact_replacement()
            }
            CommonMetadataOrSubtype::SchemaDefinition(val) => {
                val.deprecated_element_has_exact_replacement()
            }
            CommonMetadataOrSubtype::TypeDefinition(val) => {
                val.deprecated_element_has_exact_replacement()
            }
            CommonMetadataOrSubtype::SubsetDefinition(val) => {
                val.deprecated_element_has_exact_replacement()
            }
            CommonMetadataOrSubtype::Definition(val) => {
                val.deprecated_element_has_exact_replacement()
            }
            CommonMetadataOrSubtype::EnumDefinition(val) => {
                val.deprecated_element_has_exact_replacement()
            }
            CommonMetadataOrSubtype::SlotDefinition(val) => {
                val.deprecated_element_has_exact_replacement()
            }
            CommonMetadataOrSubtype::ClassDefinition(val) => {
                val.deprecated_element_has_exact_replacement()
            }
        }
    }
    fn deprecated_element_has_possible_replacement<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        match self {
            CommonMetadataOrSubtype::Element(val) => {
                val.deprecated_element_has_possible_replacement()
            }
            CommonMetadataOrSubtype::EnumBinding(val) => {
                val.deprecated_element_has_possible_replacement()
            }
            CommonMetadataOrSubtype::StructuredAlias(val) => {
                val.deprecated_element_has_possible_replacement()
            }
            CommonMetadataOrSubtype::AnonymousExpression(val) => {
                val.deprecated_element_has_possible_replacement()
            }
            CommonMetadataOrSubtype::PathExpression(val) => {
                val.deprecated_element_has_possible_replacement()
            }
            CommonMetadataOrSubtype::ClassRule(val) => {
                val.deprecated_element_has_possible_replacement()
            }
            CommonMetadataOrSubtype::ArrayExpression(val) => {
                val.deprecated_element_has_possible_replacement()
            }
            CommonMetadataOrSubtype::DimensionExpression(val) => {
                val.deprecated_element_has_possible_replacement()
            }
            CommonMetadataOrSubtype::PatternExpression(val) => {
                val.deprecated_element_has_possible_replacement()
            }
            CommonMetadataOrSubtype::ImportExpression(val) => {
                val.deprecated_element_has_possible_replacement()
            }
            CommonMetadataOrSubtype::PermissibleValue(val) => {
                val.deprecated_element_has_possible_replacement()
            }
            CommonMetadataOrSubtype::UniqueKey(val) => {
                val.deprecated_element_has_possible_replacement()
            }
            CommonMetadataOrSubtype::TypeMapping(val) => {
                val.deprecated_element_has_possible_replacement()
            }
            CommonMetadataOrSubtype::AnonymousSlotExpression(val) => {
                val.deprecated_element_has_possible_replacement()
            }
            CommonMetadataOrSubtype::AnonymousClassExpression(val) => {
                val.deprecated_element_has_possible_replacement()
            }
            CommonMetadataOrSubtype::SchemaDefinition(val) => {
                val.deprecated_element_has_possible_replacement()
            }
            CommonMetadataOrSubtype::TypeDefinition(val) => {
                val.deprecated_element_has_possible_replacement()
            }
            CommonMetadataOrSubtype::SubsetDefinition(val) => {
                val.deprecated_element_has_possible_replacement()
            }
            CommonMetadataOrSubtype::Definition(val) => {
                val.deprecated_element_has_possible_replacement()
            }
            CommonMetadataOrSubtype::EnumDefinition(val) => {
                val.deprecated_element_has_possible_replacement()
            }
            CommonMetadataOrSubtype::SlotDefinition(val) => {
                val.deprecated_element_has_possible_replacement()
            }
            CommonMetadataOrSubtype::ClassDefinition(val) => {
                val.deprecated_element_has_possible_replacement()
            }
        }
    }
    fn aliases<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        match self {
            CommonMetadataOrSubtype::Element(val) => val.aliases().map(|x| x.to_any()),
            CommonMetadataOrSubtype::EnumBinding(val) => val.aliases().map(|x| x.to_any()),
            CommonMetadataOrSubtype::StructuredAlias(val) => val.aliases().map(|x| x.to_any()),
            CommonMetadataOrSubtype::AnonymousExpression(val) => val.aliases().map(|x| x.to_any()),
            CommonMetadataOrSubtype::PathExpression(val) => val.aliases().map(|x| x.to_any()),
            CommonMetadataOrSubtype::ClassRule(val) => val.aliases().map(|x| x.to_any()),
            CommonMetadataOrSubtype::ArrayExpression(val) => val.aliases().map(|x| x.to_any()),
            CommonMetadataOrSubtype::DimensionExpression(val) => val.aliases().map(|x| x.to_any()),
            CommonMetadataOrSubtype::PatternExpression(val) => val.aliases().map(|x| x.to_any()),
            CommonMetadataOrSubtype::ImportExpression(val) => val.aliases().map(|x| x.to_any()),
            CommonMetadataOrSubtype::PermissibleValue(val) => val.aliases().map(|x| x.to_any()),
            CommonMetadataOrSubtype::UniqueKey(val) => val.aliases().map(|x| x.to_any()),
            CommonMetadataOrSubtype::TypeMapping(val) => val.aliases().map(|x| x.to_any()),
            CommonMetadataOrSubtype::AnonymousSlotExpression(val) => {
                val.aliases().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::AnonymousClassExpression(val) => {
                val.aliases().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::SchemaDefinition(val) => val.aliases().map(|x| x.to_any()),
            CommonMetadataOrSubtype::TypeDefinition(val) => val.aliases().map(|x| x.to_any()),
            CommonMetadataOrSubtype::SubsetDefinition(val) => val.aliases().map(|x| x.to_any()),
            CommonMetadataOrSubtype::Definition(val) => val.aliases().map(|x| x.to_any()),
            CommonMetadataOrSubtype::EnumDefinition(val) => val.aliases().map(|x| x.to_any()),
            CommonMetadataOrSubtype::SlotDefinition(val) => val.aliases().map(|x| x.to_any()),
            CommonMetadataOrSubtype::ClassDefinition(val) => val.aliases().map(|x| x.to_any()),
        }
    }
    fn structured_aliases<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::StructuredAlias>> {
        match self {
            CommonMetadataOrSubtype::Element(val) => val.structured_aliases().map(|x| x.to_any()),
            CommonMetadataOrSubtype::EnumBinding(val) => {
                val.structured_aliases().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::StructuredAlias(val) => {
                val.structured_aliases().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::AnonymousExpression(val) => {
                val.structured_aliases().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::PathExpression(val) => {
                val.structured_aliases().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::ClassRule(val) => val.structured_aliases().map(|x| x.to_any()),
            CommonMetadataOrSubtype::ArrayExpression(val) => {
                val.structured_aliases().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::DimensionExpression(val) => {
                val.structured_aliases().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::PatternExpression(val) => {
                val.structured_aliases().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::ImportExpression(val) => {
                val.structured_aliases().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::PermissibleValue(val) => {
                val.structured_aliases().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::UniqueKey(val) => val.structured_aliases().map(|x| x.to_any()),
            CommonMetadataOrSubtype::TypeMapping(val) => {
                val.structured_aliases().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::AnonymousSlotExpression(val) => {
                val.structured_aliases().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::AnonymousClassExpression(val) => {
                val.structured_aliases().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::SchemaDefinition(val) => {
                val.structured_aliases().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::TypeDefinition(val) => {
                val.structured_aliases().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::SubsetDefinition(val) => {
                val.structured_aliases().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::Definition(val) => {
                val.structured_aliases().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::EnumDefinition(val) => {
                val.structured_aliases().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::SlotDefinition(val) => {
                val.structured_aliases().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::ClassDefinition(val) => {
                val.structured_aliases().map(|x| x.to_any())
            }
        }
    }
    fn mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        match self {
            CommonMetadataOrSubtype::Element(val) => val.mappings().map(|x| x.to_any()),
            CommonMetadataOrSubtype::EnumBinding(val) => val.mappings().map(|x| x.to_any()),
            CommonMetadataOrSubtype::StructuredAlias(val) => val.mappings().map(|x| x.to_any()),
            CommonMetadataOrSubtype::AnonymousExpression(val) => val.mappings().map(|x| x.to_any()),
            CommonMetadataOrSubtype::PathExpression(val) => val.mappings().map(|x| x.to_any()),
            CommonMetadataOrSubtype::ClassRule(val) => val.mappings().map(|x| x.to_any()),
            CommonMetadataOrSubtype::ArrayExpression(val) => val.mappings().map(|x| x.to_any()),
            CommonMetadataOrSubtype::DimensionExpression(val) => val.mappings().map(|x| x.to_any()),
            CommonMetadataOrSubtype::PatternExpression(val) => val.mappings().map(|x| x.to_any()),
            CommonMetadataOrSubtype::ImportExpression(val) => val.mappings().map(|x| x.to_any()),
            CommonMetadataOrSubtype::PermissibleValue(val) => val.mappings().map(|x| x.to_any()),
            CommonMetadataOrSubtype::UniqueKey(val) => val.mappings().map(|x| x.to_any()),
            CommonMetadataOrSubtype::TypeMapping(val) => val.mappings().map(|x| x.to_any()),
            CommonMetadataOrSubtype::AnonymousSlotExpression(val) => {
                val.mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::AnonymousClassExpression(val) => {
                val.mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::SchemaDefinition(val) => val.mappings().map(|x| x.to_any()),
            CommonMetadataOrSubtype::TypeDefinition(val) => val.mappings().map(|x| x.to_any()),
            CommonMetadataOrSubtype::SubsetDefinition(val) => val.mappings().map(|x| x.to_any()),
            CommonMetadataOrSubtype::Definition(val) => val.mappings().map(|x| x.to_any()),
            CommonMetadataOrSubtype::EnumDefinition(val) => val.mappings().map(|x| x.to_any()),
            CommonMetadataOrSubtype::SlotDefinition(val) => val.mappings().map(|x| x.to_any()),
            CommonMetadataOrSubtype::ClassDefinition(val) => val.mappings().map(|x| x.to_any()),
        }
    }
    fn exact_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        match self {
            CommonMetadataOrSubtype::Element(val) => val.exact_mappings().map(|x| x.to_any()),
            CommonMetadataOrSubtype::EnumBinding(val) => val.exact_mappings().map(|x| x.to_any()),
            CommonMetadataOrSubtype::StructuredAlias(val) => {
                val.exact_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::AnonymousExpression(val) => {
                val.exact_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::PathExpression(val) => {
                val.exact_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::ClassRule(val) => val.exact_mappings().map(|x| x.to_any()),
            CommonMetadataOrSubtype::ArrayExpression(val) => {
                val.exact_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::DimensionExpression(val) => {
                val.exact_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::PatternExpression(val) => {
                val.exact_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::ImportExpression(val) => {
                val.exact_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::PermissibleValue(val) => {
                val.exact_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::UniqueKey(val) => val.exact_mappings().map(|x| x.to_any()),
            CommonMetadataOrSubtype::TypeMapping(val) => val.exact_mappings().map(|x| x.to_any()),
            CommonMetadataOrSubtype::AnonymousSlotExpression(val) => {
                val.exact_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::AnonymousClassExpression(val) => {
                val.exact_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::SchemaDefinition(val) => {
                val.exact_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::TypeDefinition(val) => {
                val.exact_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::SubsetDefinition(val) => {
                val.exact_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::Definition(val) => val.exact_mappings().map(|x| x.to_any()),
            CommonMetadataOrSubtype::EnumDefinition(val) => {
                val.exact_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::SlotDefinition(val) => {
                val.exact_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::ClassDefinition(val) => {
                val.exact_mappings().map(|x| x.to_any())
            }
        }
    }
    fn close_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        match self {
            CommonMetadataOrSubtype::Element(val) => val.close_mappings().map(|x| x.to_any()),
            CommonMetadataOrSubtype::EnumBinding(val) => val.close_mappings().map(|x| x.to_any()),
            CommonMetadataOrSubtype::StructuredAlias(val) => {
                val.close_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::AnonymousExpression(val) => {
                val.close_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::PathExpression(val) => {
                val.close_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::ClassRule(val) => val.close_mappings().map(|x| x.to_any()),
            CommonMetadataOrSubtype::ArrayExpression(val) => {
                val.close_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::DimensionExpression(val) => {
                val.close_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::PatternExpression(val) => {
                val.close_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::ImportExpression(val) => {
                val.close_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::PermissibleValue(val) => {
                val.close_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::UniqueKey(val) => val.close_mappings().map(|x| x.to_any()),
            CommonMetadataOrSubtype::TypeMapping(val) => val.close_mappings().map(|x| x.to_any()),
            CommonMetadataOrSubtype::AnonymousSlotExpression(val) => {
                val.close_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::AnonymousClassExpression(val) => {
                val.close_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::SchemaDefinition(val) => {
                val.close_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::TypeDefinition(val) => {
                val.close_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::SubsetDefinition(val) => {
                val.close_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::Definition(val) => val.close_mappings().map(|x| x.to_any()),
            CommonMetadataOrSubtype::EnumDefinition(val) => {
                val.close_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::SlotDefinition(val) => {
                val.close_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::ClassDefinition(val) => {
                val.close_mappings().map(|x| x.to_any())
            }
        }
    }
    fn related_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        match self {
            CommonMetadataOrSubtype::Element(val) => val.related_mappings().map(|x| x.to_any()),
            CommonMetadataOrSubtype::EnumBinding(val) => val.related_mappings().map(|x| x.to_any()),
            CommonMetadataOrSubtype::StructuredAlias(val) => {
                val.related_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::AnonymousExpression(val) => {
                val.related_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::PathExpression(val) => {
                val.related_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::ClassRule(val) => val.related_mappings().map(|x| x.to_any()),
            CommonMetadataOrSubtype::ArrayExpression(val) => {
                val.related_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::DimensionExpression(val) => {
                val.related_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::PatternExpression(val) => {
                val.related_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::ImportExpression(val) => {
                val.related_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::PermissibleValue(val) => {
                val.related_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::UniqueKey(val) => val.related_mappings().map(|x| x.to_any()),
            CommonMetadataOrSubtype::TypeMapping(val) => val.related_mappings().map(|x| x.to_any()),
            CommonMetadataOrSubtype::AnonymousSlotExpression(val) => {
                val.related_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::AnonymousClassExpression(val) => {
                val.related_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::SchemaDefinition(val) => {
                val.related_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::TypeDefinition(val) => {
                val.related_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::SubsetDefinition(val) => {
                val.related_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::Definition(val) => val.related_mappings().map(|x| x.to_any()),
            CommonMetadataOrSubtype::EnumDefinition(val) => {
                val.related_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::SlotDefinition(val) => {
                val.related_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::ClassDefinition(val) => {
                val.related_mappings().map(|x| x.to_any())
            }
        }
    }
    fn narrow_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        match self {
            CommonMetadataOrSubtype::Element(val) => val.narrow_mappings().map(|x| x.to_any()),
            CommonMetadataOrSubtype::EnumBinding(val) => val.narrow_mappings().map(|x| x.to_any()),
            CommonMetadataOrSubtype::StructuredAlias(val) => {
                val.narrow_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::AnonymousExpression(val) => {
                val.narrow_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::PathExpression(val) => {
                val.narrow_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::ClassRule(val) => val.narrow_mappings().map(|x| x.to_any()),
            CommonMetadataOrSubtype::ArrayExpression(val) => {
                val.narrow_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::DimensionExpression(val) => {
                val.narrow_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::PatternExpression(val) => {
                val.narrow_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::ImportExpression(val) => {
                val.narrow_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::PermissibleValue(val) => {
                val.narrow_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::UniqueKey(val) => val.narrow_mappings().map(|x| x.to_any()),
            CommonMetadataOrSubtype::TypeMapping(val) => val.narrow_mappings().map(|x| x.to_any()),
            CommonMetadataOrSubtype::AnonymousSlotExpression(val) => {
                val.narrow_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::AnonymousClassExpression(val) => {
                val.narrow_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::SchemaDefinition(val) => {
                val.narrow_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::TypeDefinition(val) => {
                val.narrow_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::SubsetDefinition(val) => {
                val.narrow_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::Definition(val) => val.narrow_mappings().map(|x| x.to_any()),
            CommonMetadataOrSubtype::EnumDefinition(val) => {
                val.narrow_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::SlotDefinition(val) => {
                val.narrow_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::ClassDefinition(val) => {
                val.narrow_mappings().map(|x| x.to_any())
            }
        }
    }
    fn broad_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        match self {
            CommonMetadataOrSubtype::Element(val) => val.broad_mappings().map(|x| x.to_any()),
            CommonMetadataOrSubtype::EnumBinding(val) => val.broad_mappings().map(|x| x.to_any()),
            CommonMetadataOrSubtype::StructuredAlias(val) => {
                val.broad_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::AnonymousExpression(val) => {
                val.broad_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::PathExpression(val) => {
                val.broad_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::ClassRule(val) => val.broad_mappings().map(|x| x.to_any()),
            CommonMetadataOrSubtype::ArrayExpression(val) => {
                val.broad_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::DimensionExpression(val) => {
                val.broad_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::PatternExpression(val) => {
                val.broad_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::ImportExpression(val) => {
                val.broad_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::PermissibleValue(val) => {
                val.broad_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::UniqueKey(val) => val.broad_mappings().map(|x| x.to_any()),
            CommonMetadataOrSubtype::TypeMapping(val) => val.broad_mappings().map(|x| x.to_any()),
            CommonMetadataOrSubtype::AnonymousSlotExpression(val) => {
                val.broad_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::AnonymousClassExpression(val) => {
                val.broad_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::SchemaDefinition(val) => {
                val.broad_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::TypeDefinition(val) => {
                val.broad_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::SubsetDefinition(val) => {
                val.broad_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::Definition(val) => val.broad_mappings().map(|x| x.to_any()),
            CommonMetadataOrSubtype::EnumDefinition(val) => {
                val.broad_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::SlotDefinition(val) => {
                val.broad_mappings().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::ClassDefinition(val) => {
                val.broad_mappings().map(|x| x.to_any())
            }
        }
    }
    fn created_by<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        match self {
            CommonMetadataOrSubtype::Element(val) => val.created_by(),
            CommonMetadataOrSubtype::EnumBinding(val) => val.created_by(),
            CommonMetadataOrSubtype::StructuredAlias(val) => val.created_by(),
            CommonMetadataOrSubtype::AnonymousExpression(val) => val.created_by(),
            CommonMetadataOrSubtype::PathExpression(val) => val.created_by(),
            CommonMetadataOrSubtype::ClassRule(val) => val.created_by(),
            CommonMetadataOrSubtype::ArrayExpression(val) => val.created_by(),
            CommonMetadataOrSubtype::DimensionExpression(val) => val.created_by(),
            CommonMetadataOrSubtype::PatternExpression(val) => val.created_by(),
            CommonMetadataOrSubtype::ImportExpression(val) => val.created_by(),
            CommonMetadataOrSubtype::PermissibleValue(val) => val.created_by(),
            CommonMetadataOrSubtype::UniqueKey(val) => val.created_by(),
            CommonMetadataOrSubtype::TypeMapping(val) => val.created_by(),
            CommonMetadataOrSubtype::AnonymousSlotExpression(val) => val.created_by(),
            CommonMetadataOrSubtype::AnonymousClassExpression(val) => val.created_by(),
            CommonMetadataOrSubtype::SchemaDefinition(val) => val.created_by(),
            CommonMetadataOrSubtype::TypeDefinition(val) => val.created_by(),
            CommonMetadataOrSubtype::SubsetDefinition(val) => val.created_by(),
            CommonMetadataOrSubtype::Definition(val) => val.created_by(),
            CommonMetadataOrSubtype::EnumDefinition(val) => val.created_by(),
            CommonMetadataOrSubtype::SlotDefinition(val) => val.created_by(),
            CommonMetadataOrSubtype::ClassDefinition(val) => val.created_by(),
        }
    }
    fn contributors<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        match self {
            CommonMetadataOrSubtype::Element(val) => val.contributors().map(|x| x.to_any()),
            CommonMetadataOrSubtype::EnumBinding(val) => val.contributors().map(|x| x.to_any()),
            CommonMetadataOrSubtype::StructuredAlias(val) => val.contributors().map(|x| x.to_any()),
            CommonMetadataOrSubtype::AnonymousExpression(val) => {
                val.contributors().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::PathExpression(val) => val.contributors().map(|x| x.to_any()),
            CommonMetadataOrSubtype::ClassRule(val) => val.contributors().map(|x| x.to_any()),
            CommonMetadataOrSubtype::ArrayExpression(val) => val.contributors().map(|x| x.to_any()),
            CommonMetadataOrSubtype::DimensionExpression(val) => {
                val.contributors().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::PatternExpression(val) => {
                val.contributors().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::ImportExpression(val) => {
                val.contributors().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::PermissibleValue(val) => {
                val.contributors().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::UniqueKey(val) => val.contributors().map(|x| x.to_any()),
            CommonMetadataOrSubtype::TypeMapping(val) => val.contributors().map(|x| x.to_any()),
            CommonMetadataOrSubtype::AnonymousSlotExpression(val) => {
                val.contributors().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::AnonymousClassExpression(val) => {
                val.contributors().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::SchemaDefinition(val) => {
                val.contributors().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::TypeDefinition(val) => val.contributors().map(|x| x.to_any()),
            CommonMetadataOrSubtype::SubsetDefinition(val) => {
                val.contributors().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::Definition(val) => val.contributors().map(|x| x.to_any()),
            CommonMetadataOrSubtype::EnumDefinition(val) => val.contributors().map(|x| x.to_any()),
            CommonMetadataOrSubtype::SlotDefinition(val) => val.contributors().map(|x| x.to_any()),
            CommonMetadataOrSubtype::ClassDefinition(val) => val.contributors().map(|x| x.to_any()),
        }
    }
    fn created_on<'a>(&'a self) -> Option<&'a crate::NaiveDateTime> {
        match self {
            CommonMetadataOrSubtype::Element(val) => val.created_on(),
            CommonMetadataOrSubtype::EnumBinding(val) => val.created_on(),
            CommonMetadataOrSubtype::StructuredAlias(val) => val.created_on(),
            CommonMetadataOrSubtype::AnonymousExpression(val) => val.created_on(),
            CommonMetadataOrSubtype::PathExpression(val) => val.created_on(),
            CommonMetadataOrSubtype::ClassRule(val) => val.created_on(),
            CommonMetadataOrSubtype::ArrayExpression(val) => val.created_on(),
            CommonMetadataOrSubtype::DimensionExpression(val) => val.created_on(),
            CommonMetadataOrSubtype::PatternExpression(val) => val.created_on(),
            CommonMetadataOrSubtype::ImportExpression(val) => val.created_on(),
            CommonMetadataOrSubtype::PermissibleValue(val) => val.created_on(),
            CommonMetadataOrSubtype::UniqueKey(val) => val.created_on(),
            CommonMetadataOrSubtype::TypeMapping(val) => val.created_on(),
            CommonMetadataOrSubtype::AnonymousSlotExpression(val) => val.created_on(),
            CommonMetadataOrSubtype::AnonymousClassExpression(val) => val.created_on(),
            CommonMetadataOrSubtype::SchemaDefinition(val) => val.created_on(),
            CommonMetadataOrSubtype::TypeDefinition(val) => val.created_on(),
            CommonMetadataOrSubtype::SubsetDefinition(val) => val.created_on(),
            CommonMetadataOrSubtype::Definition(val) => val.created_on(),
            CommonMetadataOrSubtype::EnumDefinition(val) => val.created_on(),
            CommonMetadataOrSubtype::SlotDefinition(val) => val.created_on(),
            CommonMetadataOrSubtype::ClassDefinition(val) => val.created_on(),
        }
    }
    fn last_updated_on<'a>(&'a self) -> Option<&'a crate::NaiveDateTime> {
        match self {
            CommonMetadataOrSubtype::Element(val) => val.last_updated_on(),
            CommonMetadataOrSubtype::EnumBinding(val) => val.last_updated_on(),
            CommonMetadataOrSubtype::StructuredAlias(val) => val.last_updated_on(),
            CommonMetadataOrSubtype::AnonymousExpression(val) => val.last_updated_on(),
            CommonMetadataOrSubtype::PathExpression(val) => val.last_updated_on(),
            CommonMetadataOrSubtype::ClassRule(val) => val.last_updated_on(),
            CommonMetadataOrSubtype::ArrayExpression(val) => val.last_updated_on(),
            CommonMetadataOrSubtype::DimensionExpression(val) => val.last_updated_on(),
            CommonMetadataOrSubtype::PatternExpression(val) => val.last_updated_on(),
            CommonMetadataOrSubtype::ImportExpression(val) => val.last_updated_on(),
            CommonMetadataOrSubtype::PermissibleValue(val) => val.last_updated_on(),
            CommonMetadataOrSubtype::UniqueKey(val) => val.last_updated_on(),
            CommonMetadataOrSubtype::TypeMapping(val) => val.last_updated_on(),
            CommonMetadataOrSubtype::AnonymousSlotExpression(val) => val.last_updated_on(),
            CommonMetadataOrSubtype::AnonymousClassExpression(val) => val.last_updated_on(),
            CommonMetadataOrSubtype::SchemaDefinition(val) => val.last_updated_on(),
            CommonMetadataOrSubtype::TypeDefinition(val) => val.last_updated_on(),
            CommonMetadataOrSubtype::SubsetDefinition(val) => val.last_updated_on(),
            CommonMetadataOrSubtype::Definition(val) => val.last_updated_on(),
            CommonMetadataOrSubtype::EnumDefinition(val) => val.last_updated_on(),
            CommonMetadataOrSubtype::SlotDefinition(val) => val.last_updated_on(),
            CommonMetadataOrSubtype::ClassDefinition(val) => val.last_updated_on(),
        }
    }
    fn modified_by<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        match self {
            CommonMetadataOrSubtype::Element(val) => val.modified_by(),
            CommonMetadataOrSubtype::EnumBinding(val) => val.modified_by(),
            CommonMetadataOrSubtype::StructuredAlias(val) => val.modified_by(),
            CommonMetadataOrSubtype::AnonymousExpression(val) => val.modified_by(),
            CommonMetadataOrSubtype::PathExpression(val) => val.modified_by(),
            CommonMetadataOrSubtype::ClassRule(val) => val.modified_by(),
            CommonMetadataOrSubtype::ArrayExpression(val) => val.modified_by(),
            CommonMetadataOrSubtype::DimensionExpression(val) => val.modified_by(),
            CommonMetadataOrSubtype::PatternExpression(val) => val.modified_by(),
            CommonMetadataOrSubtype::ImportExpression(val) => val.modified_by(),
            CommonMetadataOrSubtype::PermissibleValue(val) => val.modified_by(),
            CommonMetadataOrSubtype::UniqueKey(val) => val.modified_by(),
            CommonMetadataOrSubtype::TypeMapping(val) => val.modified_by(),
            CommonMetadataOrSubtype::AnonymousSlotExpression(val) => val.modified_by(),
            CommonMetadataOrSubtype::AnonymousClassExpression(val) => val.modified_by(),
            CommonMetadataOrSubtype::SchemaDefinition(val) => val.modified_by(),
            CommonMetadataOrSubtype::TypeDefinition(val) => val.modified_by(),
            CommonMetadataOrSubtype::SubsetDefinition(val) => val.modified_by(),
            CommonMetadataOrSubtype::Definition(val) => val.modified_by(),
            CommonMetadataOrSubtype::EnumDefinition(val) => val.modified_by(),
            CommonMetadataOrSubtype::SlotDefinition(val) => val.modified_by(),
            CommonMetadataOrSubtype::ClassDefinition(val) => val.modified_by(),
        }
    }
    fn status<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        match self {
            CommonMetadataOrSubtype::Element(val) => val.status(),
            CommonMetadataOrSubtype::EnumBinding(val) => val.status(),
            CommonMetadataOrSubtype::StructuredAlias(val) => val.status(),
            CommonMetadataOrSubtype::AnonymousExpression(val) => val.status(),
            CommonMetadataOrSubtype::PathExpression(val) => val.status(),
            CommonMetadataOrSubtype::ClassRule(val) => val.status(),
            CommonMetadataOrSubtype::ArrayExpression(val) => val.status(),
            CommonMetadataOrSubtype::DimensionExpression(val) => val.status(),
            CommonMetadataOrSubtype::PatternExpression(val) => val.status(),
            CommonMetadataOrSubtype::ImportExpression(val) => val.status(),
            CommonMetadataOrSubtype::PermissibleValue(val) => val.status(),
            CommonMetadataOrSubtype::UniqueKey(val) => val.status(),
            CommonMetadataOrSubtype::TypeMapping(val) => val.status(),
            CommonMetadataOrSubtype::AnonymousSlotExpression(val) => val.status(),
            CommonMetadataOrSubtype::AnonymousClassExpression(val) => val.status(),
            CommonMetadataOrSubtype::SchemaDefinition(val) => val.status(),
            CommonMetadataOrSubtype::TypeDefinition(val) => val.status(),
            CommonMetadataOrSubtype::SubsetDefinition(val) => val.status(),
            CommonMetadataOrSubtype::Definition(val) => val.status(),
            CommonMetadataOrSubtype::EnumDefinition(val) => val.status(),
            CommonMetadataOrSubtype::SlotDefinition(val) => val.status(),
            CommonMetadataOrSubtype::ClassDefinition(val) => val.status(),
        }
    }
    fn rank(&self) -> Option<isize> {
        match self {
            CommonMetadataOrSubtype::Element(val) => val.rank(),
            CommonMetadataOrSubtype::EnumBinding(val) => val.rank(),
            CommonMetadataOrSubtype::StructuredAlias(val) => val.rank(),
            CommonMetadataOrSubtype::AnonymousExpression(val) => val.rank(),
            CommonMetadataOrSubtype::PathExpression(val) => val.rank(),
            CommonMetadataOrSubtype::ClassRule(val) => val.rank(),
            CommonMetadataOrSubtype::ArrayExpression(val) => val.rank(),
            CommonMetadataOrSubtype::DimensionExpression(val) => val.rank(),
            CommonMetadataOrSubtype::PatternExpression(val) => val.rank(),
            CommonMetadataOrSubtype::ImportExpression(val) => val.rank(),
            CommonMetadataOrSubtype::PermissibleValue(val) => val.rank(),
            CommonMetadataOrSubtype::UniqueKey(val) => val.rank(),
            CommonMetadataOrSubtype::TypeMapping(val) => val.rank(),
            CommonMetadataOrSubtype::AnonymousSlotExpression(val) => val.rank(),
            CommonMetadataOrSubtype::AnonymousClassExpression(val) => val.rank(),
            CommonMetadataOrSubtype::SchemaDefinition(val) => val.rank(),
            CommonMetadataOrSubtype::TypeDefinition(val) => val.rank(),
            CommonMetadataOrSubtype::SubsetDefinition(val) => val.rank(),
            CommonMetadataOrSubtype::Definition(val) => val.rank(),
            CommonMetadataOrSubtype::EnumDefinition(val) => val.rank(),
            CommonMetadataOrSubtype::SlotDefinition(val) => val.rank(),
            CommonMetadataOrSubtype::ClassDefinition(val) => val.rank(),
        }
    }
    fn categories<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        match self {
            CommonMetadataOrSubtype::Element(val) => val.categories().map(|x| x.to_any()),
            CommonMetadataOrSubtype::EnumBinding(val) => val.categories().map(|x| x.to_any()),
            CommonMetadataOrSubtype::StructuredAlias(val) => val.categories().map(|x| x.to_any()),
            CommonMetadataOrSubtype::AnonymousExpression(val) => {
                val.categories().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::PathExpression(val) => val.categories().map(|x| x.to_any()),
            CommonMetadataOrSubtype::ClassRule(val) => val.categories().map(|x| x.to_any()),
            CommonMetadataOrSubtype::ArrayExpression(val) => val.categories().map(|x| x.to_any()),
            CommonMetadataOrSubtype::DimensionExpression(val) => {
                val.categories().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::PatternExpression(val) => val.categories().map(|x| x.to_any()),
            CommonMetadataOrSubtype::ImportExpression(val) => val.categories().map(|x| x.to_any()),
            CommonMetadataOrSubtype::PermissibleValue(val) => val.categories().map(|x| x.to_any()),
            CommonMetadataOrSubtype::UniqueKey(val) => val.categories().map(|x| x.to_any()),
            CommonMetadataOrSubtype::TypeMapping(val) => val.categories().map(|x| x.to_any()),
            CommonMetadataOrSubtype::AnonymousSlotExpression(val) => {
                val.categories().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::AnonymousClassExpression(val) => {
                val.categories().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::SchemaDefinition(val) => val.categories().map(|x| x.to_any()),
            CommonMetadataOrSubtype::TypeDefinition(val) => val.categories().map(|x| x.to_any()),
            CommonMetadataOrSubtype::SubsetDefinition(val) => val.categories().map(|x| x.to_any()),
            CommonMetadataOrSubtype::Definition(val) => val.categories().map(|x| x.to_any()),
            CommonMetadataOrSubtype::EnumDefinition(val) => val.categories().map(|x| x.to_any()),
            CommonMetadataOrSubtype::SlotDefinition(val) => val.categories().map(|x| x.to_any()),
            CommonMetadataOrSubtype::ClassDefinition(val) => val.categories().map(|x| x.to_any()),
        }
    }
    fn keywords<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        match self {
            CommonMetadataOrSubtype::Element(val) => val.keywords().map(|x| x.to_any()),
            CommonMetadataOrSubtype::EnumBinding(val) => val.keywords().map(|x| x.to_any()),
            CommonMetadataOrSubtype::StructuredAlias(val) => val.keywords().map(|x| x.to_any()),
            CommonMetadataOrSubtype::AnonymousExpression(val) => val.keywords().map(|x| x.to_any()),
            CommonMetadataOrSubtype::PathExpression(val) => val.keywords().map(|x| x.to_any()),
            CommonMetadataOrSubtype::ClassRule(val) => val.keywords().map(|x| x.to_any()),
            CommonMetadataOrSubtype::ArrayExpression(val) => val.keywords().map(|x| x.to_any()),
            CommonMetadataOrSubtype::DimensionExpression(val) => val.keywords().map(|x| x.to_any()),
            CommonMetadataOrSubtype::PatternExpression(val) => val.keywords().map(|x| x.to_any()),
            CommonMetadataOrSubtype::ImportExpression(val) => val.keywords().map(|x| x.to_any()),
            CommonMetadataOrSubtype::PermissibleValue(val) => val.keywords().map(|x| x.to_any()),
            CommonMetadataOrSubtype::UniqueKey(val) => val.keywords().map(|x| x.to_any()),
            CommonMetadataOrSubtype::TypeMapping(val) => val.keywords().map(|x| x.to_any()),
            CommonMetadataOrSubtype::AnonymousSlotExpression(val) => {
                val.keywords().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::AnonymousClassExpression(val) => {
                val.keywords().map(|x| x.to_any())
            }
            CommonMetadataOrSubtype::SchemaDefinition(val) => val.keywords().map(|x| x.to_any()),
            CommonMetadataOrSubtype::TypeDefinition(val) => val.keywords().map(|x| x.to_any()),
            CommonMetadataOrSubtype::SubsetDefinition(val) => val.keywords().map(|x| x.to_any()),
            CommonMetadataOrSubtype::Definition(val) => val.keywords().map(|x| x.to_any()),
            CommonMetadataOrSubtype::EnumDefinition(val) => val.keywords().map(|x| x.to_any()),
            CommonMetadataOrSubtype::SlotDefinition(val) => val.keywords().map(|x| x.to_any()),
            CommonMetadataOrSubtype::ClassDefinition(val) => val.keywords().map(|x| x.to_any()),
        }
    }
}
impl CommonMetadata for crate::ElementOrSubtype {
    fn description<'a>(&'a self) -> Option<&'a str> {
        match self {
            ElementOrSubtype::SchemaDefinition(val) => val.description(),
            ElementOrSubtype::TypeDefinition(val) => val.description(),
            ElementOrSubtype::SubsetDefinition(val) => val.description(),
            ElementOrSubtype::Definition(val) => val.description(),
            ElementOrSubtype::EnumDefinition(val) => val.description(),
            ElementOrSubtype::SlotDefinition(val) => val.description(),
            ElementOrSubtype::ClassDefinition(val) => val.description(),
        }
    }
    fn alt_descriptions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::AltDescription>> {
        match self {
            ElementOrSubtype::SchemaDefinition(val) => val.alt_descriptions().map(|x| x.to_any()),
            ElementOrSubtype::TypeDefinition(val) => val.alt_descriptions().map(|x| x.to_any()),
            ElementOrSubtype::SubsetDefinition(val) => val.alt_descriptions().map(|x| x.to_any()),
            ElementOrSubtype::Definition(val) => val.alt_descriptions().map(|x| x.to_any()),
            ElementOrSubtype::EnumDefinition(val) => val.alt_descriptions().map(|x| x.to_any()),
            ElementOrSubtype::SlotDefinition(val) => val.alt_descriptions().map(|x| x.to_any()),
            ElementOrSubtype::ClassDefinition(val) => val.alt_descriptions().map(|x| x.to_any()),
        }
    }
    fn title<'a>(&'a self) -> Option<&'a str> {
        match self {
            ElementOrSubtype::SchemaDefinition(val) => val.title(),
            ElementOrSubtype::TypeDefinition(val) => val.title(),
            ElementOrSubtype::SubsetDefinition(val) => val.title(),
            ElementOrSubtype::Definition(val) => val.title(),
            ElementOrSubtype::EnumDefinition(val) => val.title(),
            ElementOrSubtype::SlotDefinition(val) => val.title(),
            ElementOrSubtype::ClassDefinition(val) => val.title(),
        }
    }
    fn deprecated<'a>(&'a self) -> Option<&'a str> {
        match self {
            ElementOrSubtype::SchemaDefinition(val) => val.deprecated(),
            ElementOrSubtype::TypeDefinition(val) => val.deprecated(),
            ElementOrSubtype::SubsetDefinition(val) => val.deprecated(),
            ElementOrSubtype::Definition(val) => val.deprecated(),
            ElementOrSubtype::EnumDefinition(val) => val.deprecated(),
            ElementOrSubtype::SlotDefinition(val) => val.deprecated(),
            ElementOrSubtype::ClassDefinition(val) => val.deprecated(),
        }
    }
    fn todos<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        match self {
            ElementOrSubtype::SchemaDefinition(val) => val.todos().map(|x| x.to_any()),
            ElementOrSubtype::TypeDefinition(val) => val.todos().map(|x| x.to_any()),
            ElementOrSubtype::SubsetDefinition(val) => val.todos().map(|x| x.to_any()),
            ElementOrSubtype::Definition(val) => val.todos().map(|x| x.to_any()),
            ElementOrSubtype::EnumDefinition(val) => val.todos().map(|x| x.to_any()),
            ElementOrSubtype::SlotDefinition(val) => val.todos().map(|x| x.to_any()),
            ElementOrSubtype::ClassDefinition(val) => val.todos().map(|x| x.to_any()),
        }
    }
    fn notes<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        match self {
            ElementOrSubtype::SchemaDefinition(val) => val.notes().map(|x| x.to_any()),
            ElementOrSubtype::TypeDefinition(val) => val.notes().map(|x| x.to_any()),
            ElementOrSubtype::SubsetDefinition(val) => val.notes().map(|x| x.to_any()),
            ElementOrSubtype::Definition(val) => val.notes().map(|x| x.to_any()),
            ElementOrSubtype::EnumDefinition(val) => val.notes().map(|x| x.to_any()),
            ElementOrSubtype::SlotDefinition(val) => val.notes().map(|x| x.to_any()),
            ElementOrSubtype::ClassDefinition(val) => val.notes().map(|x| x.to_any()),
        }
    }
    fn comments<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        match self {
            ElementOrSubtype::SchemaDefinition(val) => val.comments().map(|x| x.to_any()),
            ElementOrSubtype::TypeDefinition(val) => val.comments().map(|x| x.to_any()),
            ElementOrSubtype::SubsetDefinition(val) => val.comments().map(|x| x.to_any()),
            ElementOrSubtype::Definition(val) => val.comments().map(|x| x.to_any()),
            ElementOrSubtype::EnumDefinition(val) => val.comments().map(|x| x.to_any()),
            ElementOrSubtype::SlotDefinition(val) => val.comments().map(|x| x.to_any()),
            ElementOrSubtype::ClassDefinition(val) => val.comments().map(|x| x.to_any()),
        }
    }
    fn examples<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::Example>> {
        match self {
            ElementOrSubtype::SchemaDefinition(val) => val.examples().map(|x| x.to_any()),
            ElementOrSubtype::TypeDefinition(val) => val.examples().map(|x| x.to_any()),
            ElementOrSubtype::SubsetDefinition(val) => val.examples().map(|x| x.to_any()),
            ElementOrSubtype::Definition(val) => val.examples().map(|x| x.to_any()),
            ElementOrSubtype::EnumDefinition(val) => val.examples().map(|x| x.to_any()),
            ElementOrSubtype::SlotDefinition(val) => val.examples().map(|x| x.to_any()),
            ElementOrSubtype::ClassDefinition(val) => val.examples().map(|x| x.to_any()),
        }
    }
    fn in_subset<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        match self {
            ElementOrSubtype::SchemaDefinition(val) => val.in_subset().map(|x| x.to_any()),
            ElementOrSubtype::TypeDefinition(val) => val.in_subset().map(|x| x.to_any()),
            ElementOrSubtype::SubsetDefinition(val) => val.in_subset().map(|x| x.to_any()),
            ElementOrSubtype::Definition(val) => val.in_subset().map(|x| x.to_any()),
            ElementOrSubtype::EnumDefinition(val) => val.in_subset().map(|x| x.to_any()),
            ElementOrSubtype::SlotDefinition(val) => val.in_subset().map(|x| x.to_any()),
            ElementOrSubtype::ClassDefinition(val) => val.in_subset().map(|x| x.to_any()),
        }
    }
    fn from_schema<'a>(&'a self) -> Option<&'a crate::uri> {
        match self {
            ElementOrSubtype::SchemaDefinition(val) => val.from_schema(),
            ElementOrSubtype::TypeDefinition(val) => val.from_schema(),
            ElementOrSubtype::SubsetDefinition(val) => val.from_schema(),
            ElementOrSubtype::Definition(val) => val.from_schema(),
            ElementOrSubtype::EnumDefinition(val) => val.from_schema(),
            ElementOrSubtype::SlotDefinition(val) => val.from_schema(),
            ElementOrSubtype::ClassDefinition(val) => val.from_schema(),
        }
    }
    fn imported_from<'a>(&'a self) -> Option<&'a str> {
        match self {
            ElementOrSubtype::SchemaDefinition(val) => val.imported_from(),
            ElementOrSubtype::TypeDefinition(val) => val.imported_from(),
            ElementOrSubtype::SubsetDefinition(val) => val.imported_from(),
            ElementOrSubtype::Definition(val) => val.imported_from(),
            ElementOrSubtype::EnumDefinition(val) => val.imported_from(),
            ElementOrSubtype::SlotDefinition(val) => val.imported_from(),
            ElementOrSubtype::ClassDefinition(val) => val.imported_from(),
        }
    }
    fn source<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        match self {
            ElementOrSubtype::SchemaDefinition(val) => val.source(),
            ElementOrSubtype::TypeDefinition(val) => val.source(),
            ElementOrSubtype::SubsetDefinition(val) => val.source(),
            ElementOrSubtype::Definition(val) => val.source(),
            ElementOrSubtype::EnumDefinition(val) => val.source(),
            ElementOrSubtype::SlotDefinition(val) => val.source(),
            ElementOrSubtype::ClassDefinition(val) => val.source(),
        }
    }
    fn in_language<'a>(&'a self) -> Option<&'a str> {
        match self {
            ElementOrSubtype::SchemaDefinition(val) => val.in_language(),
            ElementOrSubtype::TypeDefinition(val) => val.in_language(),
            ElementOrSubtype::SubsetDefinition(val) => val.in_language(),
            ElementOrSubtype::Definition(val) => val.in_language(),
            ElementOrSubtype::EnumDefinition(val) => val.in_language(),
            ElementOrSubtype::SlotDefinition(val) => val.in_language(),
            ElementOrSubtype::ClassDefinition(val) => val.in_language(),
        }
    }
    fn see_also<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        match self {
            ElementOrSubtype::SchemaDefinition(val) => val.see_also().map(|x| x.to_any()),
            ElementOrSubtype::TypeDefinition(val) => val.see_also().map(|x| x.to_any()),
            ElementOrSubtype::SubsetDefinition(val) => val.see_also().map(|x| x.to_any()),
            ElementOrSubtype::Definition(val) => val.see_also().map(|x| x.to_any()),
            ElementOrSubtype::EnumDefinition(val) => val.see_also().map(|x| x.to_any()),
            ElementOrSubtype::SlotDefinition(val) => val.see_also().map(|x| x.to_any()),
            ElementOrSubtype::ClassDefinition(val) => val.see_also().map(|x| x.to_any()),
        }
    }
    fn deprecated_element_has_exact_replacement<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        match self {
            ElementOrSubtype::SchemaDefinition(val) => {
                val.deprecated_element_has_exact_replacement()
            }
            ElementOrSubtype::TypeDefinition(val) => val.deprecated_element_has_exact_replacement(),
            ElementOrSubtype::SubsetDefinition(val) => {
                val.deprecated_element_has_exact_replacement()
            }
            ElementOrSubtype::Definition(val) => val.deprecated_element_has_exact_replacement(),
            ElementOrSubtype::EnumDefinition(val) => val.deprecated_element_has_exact_replacement(),
            ElementOrSubtype::SlotDefinition(val) => val.deprecated_element_has_exact_replacement(),
            ElementOrSubtype::ClassDefinition(val) => {
                val.deprecated_element_has_exact_replacement()
            }
        }
    }
    fn deprecated_element_has_possible_replacement<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        match self {
            ElementOrSubtype::SchemaDefinition(val) => {
                val.deprecated_element_has_possible_replacement()
            }
            ElementOrSubtype::TypeDefinition(val) => {
                val.deprecated_element_has_possible_replacement()
            }
            ElementOrSubtype::SubsetDefinition(val) => {
                val.deprecated_element_has_possible_replacement()
            }
            ElementOrSubtype::Definition(val) => val.deprecated_element_has_possible_replacement(),
            ElementOrSubtype::EnumDefinition(val) => {
                val.deprecated_element_has_possible_replacement()
            }
            ElementOrSubtype::SlotDefinition(val) => {
                val.deprecated_element_has_possible_replacement()
            }
            ElementOrSubtype::ClassDefinition(val) => {
                val.deprecated_element_has_possible_replacement()
            }
        }
    }
    fn aliases<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        match self {
            ElementOrSubtype::SchemaDefinition(val) => val.aliases().map(|x| x.to_any()),
            ElementOrSubtype::TypeDefinition(val) => val.aliases().map(|x| x.to_any()),
            ElementOrSubtype::SubsetDefinition(val) => val.aliases().map(|x| x.to_any()),
            ElementOrSubtype::Definition(val) => val.aliases().map(|x| x.to_any()),
            ElementOrSubtype::EnumDefinition(val) => val.aliases().map(|x| x.to_any()),
            ElementOrSubtype::SlotDefinition(val) => val.aliases().map(|x| x.to_any()),
            ElementOrSubtype::ClassDefinition(val) => val.aliases().map(|x| x.to_any()),
        }
    }
    fn structured_aliases<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::StructuredAlias>> {
        match self {
            ElementOrSubtype::SchemaDefinition(val) => val.structured_aliases().map(|x| x.to_any()),
            ElementOrSubtype::TypeDefinition(val) => val.structured_aliases().map(|x| x.to_any()),
            ElementOrSubtype::SubsetDefinition(val) => val.structured_aliases().map(|x| x.to_any()),
            ElementOrSubtype::Definition(val) => val.structured_aliases().map(|x| x.to_any()),
            ElementOrSubtype::EnumDefinition(val) => val.structured_aliases().map(|x| x.to_any()),
            ElementOrSubtype::SlotDefinition(val) => val.structured_aliases().map(|x| x.to_any()),
            ElementOrSubtype::ClassDefinition(val) => val.structured_aliases().map(|x| x.to_any()),
        }
    }
    fn mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        match self {
            ElementOrSubtype::SchemaDefinition(val) => val.mappings().map(|x| x.to_any()),
            ElementOrSubtype::TypeDefinition(val) => val.mappings().map(|x| x.to_any()),
            ElementOrSubtype::SubsetDefinition(val) => val.mappings().map(|x| x.to_any()),
            ElementOrSubtype::Definition(val) => val.mappings().map(|x| x.to_any()),
            ElementOrSubtype::EnumDefinition(val) => val.mappings().map(|x| x.to_any()),
            ElementOrSubtype::SlotDefinition(val) => val.mappings().map(|x| x.to_any()),
            ElementOrSubtype::ClassDefinition(val) => val.mappings().map(|x| x.to_any()),
        }
    }
    fn exact_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        match self {
            ElementOrSubtype::SchemaDefinition(val) => val.exact_mappings().map(|x| x.to_any()),
            ElementOrSubtype::TypeDefinition(val) => val.exact_mappings().map(|x| x.to_any()),
            ElementOrSubtype::SubsetDefinition(val) => val.exact_mappings().map(|x| x.to_any()),
            ElementOrSubtype::Definition(val) => val.exact_mappings().map(|x| x.to_any()),
            ElementOrSubtype::EnumDefinition(val) => val.exact_mappings().map(|x| x.to_any()),
            ElementOrSubtype::SlotDefinition(val) => val.exact_mappings().map(|x| x.to_any()),
            ElementOrSubtype::ClassDefinition(val) => val.exact_mappings().map(|x| x.to_any()),
        }
    }
    fn close_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        match self {
            ElementOrSubtype::SchemaDefinition(val) => val.close_mappings().map(|x| x.to_any()),
            ElementOrSubtype::TypeDefinition(val) => val.close_mappings().map(|x| x.to_any()),
            ElementOrSubtype::SubsetDefinition(val) => val.close_mappings().map(|x| x.to_any()),
            ElementOrSubtype::Definition(val) => val.close_mappings().map(|x| x.to_any()),
            ElementOrSubtype::EnumDefinition(val) => val.close_mappings().map(|x| x.to_any()),
            ElementOrSubtype::SlotDefinition(val) => val.close_mappings().map(|x| x.to_any()),
            ElementOrSubtype::ClassDefinition(val) => val.close_mappings().map(|x| x.to_any()),
        }
    }
    fn related_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        match self {
            ElementOrSubtype::SchemaDefinition(val) => val.related_mappings().map(|x| x.to_any()),
            ElementOrSubtype::TypeDefinition(val) => val.related_mappings().map(|x| x.to_any()),
            ElementOrSubtype::SubsetDefinition(val) => val.related_mappings().map(|x| x.to_any()),
            ElementOrSubtype::Definition(val) => val.related_mappings().map(|x| x.to_any()),
            ElementOrSubtype::EnumDefinition(val) => val.related_mappings().map(|x| x.to_any()),
            ElementOrSubtype::SlotDefinition(val) => val.related_mappings().map(|x| x.to_any()),
            ElementOrSubtype::ClassDefinition(val) => val.related_mappings().map(|x| x.to_any()),
        }
    }
    fn narrow_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        match self {
            ElementOrSubtype::SchemaDefinition(val) => val.narrow_mappings().map(|x| x.to_any()),
            ElementOrSubtype::TypeDefinition(val) => val.narrow_mappings().map(|x| x.to_any()),
            ElementOrSubtype::SubsetDefinition(val) => val.narrow_mappings().map(|x| x.to_any()),
            ElementOrSubtype::Definition(val) => val.narrow_mappings().map(|x| x.to_any()),
            ElementOrSubtype::EnumDefinition(val) => val.narrow_mappings().map(|x| x.to_any()),
            ElementOrSubtype::SlotDefinition(val) => val.narrow_mappings().map(|x| x.to_any()),
            ElementOrSubtype::ClassDefinition(val) => val.narrow_mappings().map(|x| x.to_any()),
        }
    }
    fn broad_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        match self {
            ElementOrSubtype::SchemaDefinition(val) => val.broad_mappings().map(|x| x.to_any()),
            ElementOrSubtype::TypeDefinition(val) => val.broad_mappings().map(|x| x.to_any()),
            ElementOrSubtype::SubsetDefinition(val) => val.broad_mappings().map(|x| x.to_any()),
            ElementOrSubtype::Definition(val) => val.broad_mappings().map(|x| x.to_any()),
            ElementOrSubtype::EnumDefinition(val) => val.broad_mappings().map(|x| x.to_any()),
            ElementOrSubtype::SlotDefinition(val) => val.broad_mappings().map(|x| x.to_any()),
            ElementOrSubtype::ClassDefinition(val) => val.broad_mappings().map(|x| x.to_any()),
        }
    }
    fn created_by<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        match self {
            ElementOrSubtype::SchemaDefinition(val) => val.created_by(),
            ElementOrSubtype::TypeDefinition(val) => val.created_by(),
            ElementOrSubtype::SubsetDefinition(val) => val.created_by(),
            ElementOrSubtype::Definition(val) => val.created_by(),
            ElementOrSubtype::EnumDefinition(val) => val.created_by(),
            ElementOrSubtype::SlotDefinition(val) => val.created_by(),
            ElementOrSubtype::ClassDefinition(val) => val.created_by(),
        }
    }
    fn contributors<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        match self {
            ElementOrSubtype::SchemaDefinition(val) => val.contributors().map(|x| x.to_any()),
            ElementOrSubtype::TypeDefinition(val) => val.contributors().map(|x| x.to_any()),
            ElementOrSubtype::SubsetDefinition(val) => val.contributors().map(|x| x.to_any()),
            ElementOrSubtype::Definition(val) => val.contributors().map(|x| x.to_any()),
            ElementOrSubtype::EnumDefinition(val) => val.contributors().map(|x| x.to_any()),
            ElementOrSubtype::SlotDefinition(val) => val.contributors().map(|x| x.to_any()),
            ElementOrSubtype::ClassDefinition(val) => val.contributors().map(|x| x.to_any()),
        }
    }
    fn created_on<'a>(&'a self) -> Option<&'a crate::NaiveDateTime> {
        match self {
            ElementOrSubtype::SchemaDefinition(val) => val.created_on(),
            ElementOrSubtype::TypeDefinition(val) => val.created_on(),
            ElementOrSubtype::SubsetDefinition(val) => val.created_on(),
            ElementOrSubtype::Definition(val) => val.created_on(),
            ElementOrSubtype::EnumDefinition(val) => val.created_on(),
            ElementOrSubtype::SlotDefinition(val) => val.created_on(),
            ElementOrSubtype::ClassDefinition(val) => val.created_on(),
        }
    }
    fn last_updated_on<'a>(&'a self) -> Option<&'a crate::NaiveDateTime> {
        match self {
            ElementOrSubtype::SchemaDefinition(val) => val.last_updated_on(),
            ElementOrSubtype::TypeDefinition(val) => val.last_updated_on(),
            ElementOrSubtype::SubsetDefinition(val) => val.last_updated_on(),
            ElementOrSubtype::Definition(val) => val.last_updated_on(),
            ElementOrSubtype::EnumDefinition(val) => val.last_updated_on(),
            ElementOrSubtype::SlotDefinition(val) => val.last_updated_on(),
            ElementOrSubtype::ClassDefinition(val) => val.last_updated_on(),
        }
    }
    fn modified_by<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        match self {
            ElementOrSubtype::SchemaDefinition(val) => val.modified_by(),
            ElementOrSubtype::TypeDefinition(val) => val.modified_by(),
            ElementOrSubtype::SubsetDefinition(val) => val.modified_by(),
            ElementOrSubtype::Definition(val) => val.modified_by(),
            ElementOrSubtype::EnumDefinition(val) => val.modified_by(),
            ElementOrSubtype::SlotDefinition(val) => val.modified_by(),
            ElementOrSubtype::ClassDefinition(val) => val.modified_by(),
        }
    }
    fn status<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        match self {
            ElementOrSubtype::SchemaDefinition(val) => val.status(),
            ElementOrSubtype::TypeDefinition(val) => val.status(),
            ElementOrSubtype::SubsetDefinition(val) => val.status(),
            ElementOrSubtype::Definition(val) => val.status(),
            ElementOrSubtype::EnumDefinition(val) => val.status(),
            ElementOrSubtype::SlotDefinition(val) => val.status(),
            ElementOrSubtype::ClassDefinition(val) => val.status(),
        }
    }
    fn rank(&self) -> Option<isize> {
        match self {
            ElementOrSubtype::SchemaDefinition(val) => val.rank(),
            ElementOrSubtype::TypeDefinition(val) => val.rank(),
            ElementOrSubtype::SubsetDefinition(val) => val.rank(),
            ElementOrSubtype::Definition(val) => val.rank(),
            ElementOrSubtype::EnumDefinition(val) => val.rank(),
            ElementOrSubtype::SlotDefinition(val) => val.rank(),
            ElementOrSubtype::ClassDefinition(val) => val.rank(),
        }
    }
    fn categories<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        match self {
            ElementOrSubtype::SchemaDefinition(val) => val.categories().map(|x| x.to_any()),
            ElementOrSubtype::TypeDefinition(val) => val.categories().map(|x| x.to_any()),
            ElementOrSubtype::SubsetDefinition(val) => val.categories().map(|x| x.to_any()),
            ElementOrSubtype::Definition(val) => val.categories().map(|x| x.to_any()),
            ElementOrSubtype::EnumDefinition(val) => val.categories().map(|x| x.to_any()),
            ElementOrSubtype::SlotDefinition(val) => val.categories().map(|x| x.to_any()),
            ElementOrSubtype::ClassDefinition(val) => val.categories().map(|x| x.to_any()),
        }
    }
    fn keywords<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        match self {
            ElementOrSubtype::SchemaDefinition(val) => val.keywords().map(|x| x.to_any()),
            ElementOrSubtype::TypeDefinition(val) => val.keywords().map(|x| x.to_any()),
            ElementOrSubtype::SubsetDefinition(val) => val.keywords().map(|x| x.to_any()),
            ElementOrSubtype::Definition(val) => val.keywords().map(|x| x.to_any()),
            ElementOrSubtype::EnumDefinition(val) => val.keywords().map(|x| x.to_any()),
            ElementOrSubtype::SlotDefinition(val) => val.keywords().map(|x| x.to_any()),
            ElementOrSubtype::ClassDefinition(val) => val.keywords().map(|x| x.to_any()),
        }
    }
}
impl CommonMetadata for crate::AnonymousExpressionOrSubtype {
    fn description<'a>(&'a self) -> Option<&'a str> {
        match self {
            AnonymousExpressionOrSubtype::AnonymousSlotExpression(val) => val.description(),
            AnonymousExpressionOrSubtype::AnonymousClassExpression(val) => val.description(),
        }
    }
    fn alt_descriptions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::AltDescription>> {
        match self {
            AnonymousExpressionOrSubtype::AnonymousSlotExpression(val) => {
                val.alt_descriptions().map(|x| x.to_any())
            }
            AnonymousExpressionOrSubtype::AnonymousClassExpression(val) => {
                val.alt_descriptions().map(|x| x.to_any())
            }
        }
    }
    fn title<'a>(&'a self) -> Option<&'a str> {
        match self {
            AnonymousExpressionOrSubtype::AnonymousSlotExpression(val) => val.title(),
            AnonymousExpressionOrSubtype::AnonymousClassExpression(val) => val.title(),
        }
    }
    fn deprecated<'a>(&'a self) -> Option<&'a str> {
        match self {
            AnonymousExpressionOrSubtype::AnonymousSlotExpression(val) => val.deprecated(),
            AnonymousExpressionOrSubtype::AnonymousClassExpression(val) => val.deprecated(),
        }
    }
    fn todos<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        match self {
            AnonymousExpressionOrSubtype::AnonymousSlotExpression(val) => {
                val.todos().map(|x| x.to_any())
            }
            AnonymousExpressionOrSubtype::AnonymousClassExpression(val) => {
                val.todos().map(|x| x.to_any())
            }
        }
    }
    fn notes<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        match self {
            AnonymousExpressionOrSubtype::AnonymousSlotExpression(val) => {
                val.notes().map(|x| x.to_any())
            }
            AnonymousExpressionOrSubtype::AnonymousClassExpression(val) => {
                val.notes().map(|x| x.to_any())
            }
        }
    }
    fn comments<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        match self {
            AnonymousExpressionOrSubtype::AnonymousSlotExpression(val) => {
                val.comments().map(|x| x.to_any())
            }
            AnonymousExpressionOrSubtype::AnonymousClassExpression(val) => {
                val.comments().map(|x| x.to_any())
            }
        }
    }
    fn examples<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::Example>> {
        match self {
            AnonymousExpressionOrSubtype::AnonymousSlotExpression(val) => {
                val.examples().map(|x| x.to_any())
            }
            AnonymousExpressionOrSubtype::AnonymousClassExpression(val) => {
                val.examples().map(|x| x.to_any())
            }
        }
    }
    fn in_subset<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        match self {
            AnonymousExpressionOrSubtype::AnonymousSlotExpression(val) => {
                val.in_subset().map(|x| x.to_any())
            }
            AnonymousExpressionOrSubtype::AnonymousClassExpression(val) => {
                val.in_subset().map(|x| x.to_any())
            }
        }
    }
    fn from_schema<'a>(&'a self) -> Option<&'a crate::uri> {
        match self {
            AnonymousExpressionOrSubtype::AnonymousSlotExpression(val) => val.from_schema(),
            AnonymousExpressionOrSubtype::AnonymousClassExpression(val) => val.from_schema(),
        }
    }
    fn imported_from<'a>(&'a self) -> Option<&'a str> {
        match self {
            AnonymousExpressionOrSubtype::AnonymousSlotExpression(val) => val.imported_from(),
            AnonymousExpressionOrSubtype::AnonymousClassExpression(val) => val.imported_from(),
        }
    }
    fn source<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        match self {
            AnonymousExpressionOrSubtype::AnonymousSlotExpression(val) => val.source(),
            AnonymousExpressionOrSubtype::AnonymousClassExpression(val) => val.source(),
        }
    }
    fn in_language<'a>(&'a self) -> Option<&'a str> {
        match self {
            AnonymousExpressionOrSubtype::AnonymousSlotExpression(val) => val.in_language(),
            AnonymousExpressionOrSubtype::AnonymousClassExpression(val) => val.in_language(),
        }
    }
    fn see_also<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        match self {
            AnonymousExpressionOrSubtype::AnonymousSlotExpression(val) => {
                val.see_also().map(|x| x.to_any())
            }
            AnonymousExpressionOrSubtype::AnonymousClassExpression(val) => {
                val.see_also().map(|x| x.to_any())
            }
        }
    }
    fn deprecated_element_has_exact_replacement<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        match self {
            AnonymousExpressionOrSubtype::AnonymousSlotExpression(val) => {
                val.deprecated_element_has_exact_replacement()
            }
            AnonymousExpressionOrSubtype::AnonymousClassExpression(val) => {
                val.deprecated_element_has_exact_replacement()
            }
        }
    }
    fn deprecated_element_has_possible_replacement<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        match self {
            AnonymousExpressionOrSubtype::AnonymousSlotExpression(val) => {
                val.deprecated_element_has_possible_replacement()
            }
            AnonymousExpressionOrSubtype::AnonymousClassExpression(val) => {
                val.deprecated_element_has_possible_replacement()
            }
        }
    }
    fn aliases<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        match self {
            AnonymousExpressionOrSubtype::AnonymousSlotExpression(val) => {
                val.aliases().map(|x| x.to_any())
            }
            AnonymousExpressionOrSubtype::AnonymousClassExpression(val) => {
                val.aliases().map(|x| x.to_any())
            }
        }
    }
    fn structured_aliases<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::StructuredAlias>> {
        match self {
            AnonymousExpressionOrSubtype::AnonymousSlotExpression(val) => {
                val.structured_aliases().map(|x| x.to_any())
            }
            AnonymousExpressionOrSubtype::AnonymousClassExpression(val) => {
                val.structured_aliases().map(|x| x.to_any())
            }
        }
    }
    fn mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        match self {
            AnonymousExpressionOrSubtype::AnonymousSlotExpression(val) => {
                val.mappings().map(|x| x.to_any())
            }
            AnonymousExpressionOrSubtype::AnonymousClassExpression(val) => {
                val.mappings().map(|x| x.to_any())
            }
        }
    }
    fn exact_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        match self {
            AnonymousExpressionOrSubtype::AnonymousSlotExpression(val) => {
                val.exact_mappings().map(|x| x.to_any())
            }
            AnonymousExpressionOrSubtype::AnonymousClassExpression(val) => {
                val.exact_mappings().map(|x| x.to_any())
            }
        }
    }
    fn close_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        match self {
            AnonymousExpressionOrSubtype::AnonymousSlotExpression(val) => {
                val.close_mappings().map(|x| x.to_any())
            }
            AnonymousExpressionOrSubtype::AnonymousClassExpression(val) => {
                val.close_mappings().map(|x| x.to_any())
            }
        }
    }
    fn related_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        match self {
            AnonymousExpressionOrSubtype::AnonymousSlotExpression(val) => {
                val.related_mappings().map(|x| x.to_any())
            }
            AnonymousExpressionOrSubtype::AnonymousClassExpression(val) => {
                val.related_mappings().map(|x| x.to_any())
            }
        }
    }
    fn narrow_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        match self {
            AnonymousExpressionOrSubtype::AnonymousSlotExpression(val) => {
                val.narrow_mappings().map(|x| x.to_any())
            }
            AnonymousExpressionOrSubtype::AnonymousClassExpression(val) => {
                val.narrow_mappings().map(|x| x.to_any())
            }
        }
    }
    fn broad_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        match self {
            AnonymousExpressionOrSubtype::AnonymousSlotExpression(val) => {
                val.broad_mappings().map(|x| x.to_any())
            }
            AnonymousExpressionOrSubtype::AnonymousClassExpression(val) => {
                val.broad_mappings().map(|x| x.to_any())
            }
        }
    }
    fn created_by<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        match self {
            AnonymousExpressionOrSubtype::AnonymousSlotExpression(val) => val.created_by(),
            AnonymousExpressionOrSubtype::AnonymousClassExpression(val) => val.created_by(),
        }
    }
    fn contributors<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        match self {
            AnonymousExpressionOrSubtype::AnonymousSlotExpression(val) => {
                val.contributors().map(|x| x.to_any())
            }
            AnonymousExpressionOrSubtype::AnonymousClassExpression(val) => {
                val.contributors().map(|x| x.to_any())
            }
        }
    }
    fn created_on<'a>(&'a self) -> Option<&'a crate::NaiveDateTime> {
        match self {
            AnonymousExpressionOrSubtype::AnonymousSlotExpression(val) => val.created_on(),
            AnonymousExpressionOrSubtype::AnonymousClassExpression(val) => val.created_on(),
        }
    }
    fn last_updated_on<'a>(&'a self) -> Option<&'a crate::NaiveDateTime> {
        match self {
            AnonymousExpressionOrSubtype::AnonymousSlotExpression(val) => val.last_updated_on(),
            AnonymousExpressionOrSubtype::AnonymousClassExpression(val) => val.last_updated_on(),
        }
    }
    fn modified_by<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        match self {
            AnonymousExpressionOrSubtype::AnonymousSlotExpression(val) => val.modified_by(),
            AnonymousExpressionOrSubtype::AnonymousClassExpression(val) => val.modified_by(),
        }
    }
    fn status<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        match self {
            AnonymousExpressionOrSubtype::AnonymousSlotExpression(val) => val.status(),
            AnonymousExpressionOrSubtype::AnonymousClassExpression(val) => val.status(),
        }
    }
    fn rank(&self) -> Option<isize> {
        match self {
            AnonymousExpressionOrSubtype::AnonymousSlotExpression(val) => val.rank(),
            AnonymousExpressionOrSubtype::AnonymousClassExpression(val) => val.rank(),
        }
    }
    fn categories<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        match self {
            AnonymousExpressionOrSubtype::AnonymousSlotExpression(val) => {
                val.categories().map(|x| x.to_any())
            }
            AnonymousExpressionOrSubtype::AnonymousClassExpression(val) => {
                val.categories().map(|x| x.to_any())
            }
        }
    }
    fn keywords<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        match self {
            AnonymousExpressionOrSubtype::AnonymousSlotExpression(val) => {
                val.keywords().map(|x| x.to_any())
            }
            AnonymousExpressionOrSubtype::AnonymousClassExpression(val) => {
                val.keywords().map(|x| x.to_any())
            }
        }
    }
}
impl CommonMetadata for crate::DefinitionOrSubtype {
    fn description<'a>(&'a self) -> Option<&'a str> {
        match self {
            DefinitionOrSubtype::EnumDefinition(val) => val.description(),
            DefinitionOrSubtype::SlotDefinition(val) => val.description(),
            DefinitionOrSubtype::ClassDefinition(val) => val.description(),
        }
    }
    fn alt_descriptions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::AltDescription>> {
        match self {
            DefinitionOrSubtype::EnumDefinition(val) => val.alt_descriptions().map(|x| x.to_any()),
            DefinitionOrSubtype::SlotDefinition(val) => val.alt_descriptions().map(|x| x.to_any()),
            DefinitionOrSubtype::ClassDefinition(val) => val.alt_descriptions().map(|x| x.to_any()),
        }
    }
    fn title<'a>(&'a self) -> Option<&'a str> {
        match self {
            DefinitionOrSubtype::EnumDefinition(val) => val.title(),
            DefinitionOrSubtype::SlotDefinition(val) => val.title(),
            DefinitionOrSubtype::ClassDefinition(val) => val.title(),
        }
    }
    fn deprecated<'a>(&'a self) -> Option<&'a str> {
        match self {
            DefinitionOrSubtype::EnumDefinition(val) => val.deprecated(),
            DefinitionOrSubtype::SlotDefinition(val) => val.deprecated(),
            DefinitionOrSubtype::ClassDefinition(val) => val.deprecated(),
        }
    }
    fn todos<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        match self {
            DefinitionOrSubtype::EnumDefinition(val) => val.todos().map(|x| x.to_any()),
            DefinitionOrSubtype::SlotDefinition(val) => val.todos().map(|x| x.to_any()),
            DefinitionOrSubtype::ClassDefinition(val) => val.todos().map(|x| x.to_any()),
        }
    }
    fn notes<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        match self {
            DefinitionOrSubtype::EnumDefinition(val) => val.notes().map(|x| x.to_any()),
            DefinitionOrSubtype::SlotDefinition(val) => val.notes().map(|x| x.to_any()),
            DefinitionOrSubtype::ClassDefinition(val) => val.notes().map(|x| x.to_any()),
        }
    }
    fn comments<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        match self {
            DefinitionOrSubtype::EnumDefinition(val) => val.comments().map(|x| x.to_any()),
            DefinitionOrSubtype::SlotDefinition(val) => val.comments().map(|x| x.to_any()),
            DefinitionOrSubtype::ClassDefinition(val) => val.comments().map(|x| x.to_any()),
        }
    }
    fn examples<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::Example>> {
        match self {
            DefinitionOrSubtype::EnumDefinition(val) => val.examples().map(|x| x.to_any()),
            DefinitionOrSubtype::SlotDefinition(val) => val.examples().map(|x| x.to_any()),
            DefinitionOrSubtype::ClassDefinition(val) => val.examples().map(|x| x.to_any()),
        }
    }
    fn in_subset<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        match self {
            DefinitionOrSubtype::EnumDefinition(val) => val.in_subset().map(|x| x.to_any()),
            DefinitionOrSubtype::SlotDefinition(val) => val.in_subset().map(|x| x.to_any()),
            DefinitionOrSubtype::ClassDefinition(val) => val.in_subset().map(|x| x.to_any()),
        }
    }
    fn from_schema<'a>(&'a self) -> Option<&'a crate::uri> {
        match self {
            DefinitionOrSubtype::EnumDefinition(val) => val.from_schema(),
            DefinitionOrSubtype::SlotDefinition(val) => val.from_schema(),
            DefinitionOrSubtype::ClassDefinition(val) => val.from_schema(),
        }
    }
    fn imported_from<'a>(&'a self) -> Option<&'a str> {
        match self {
            DefinitionOrSubtype::EnumDefinition(val) => val.imported_from(),
            DefinitionOrSubtype::SlotDefinition(val) => val.imported_from(),
            DefinitionOrSubtype::ClassDefinition(val) => val.imported_from(),
        }
    }
    fn source<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        match self {
            DefinitionOrSubtype::EnumDefinition(val) => val.source(),
            DefinitionOrSubtype::SlotDefinition(val) => val.source(),
            DefinitionOrSubtype::ClassDefinition(val) => val.source(),
        }
    }
    fn in_language<'a>(&'a self) -> Option<&'a str> {
        match self {
            DefinitionOrSubtype::EnumDefinition(val) => val.in_language(),
            DefinitionOrSubtype::SlotDefinition(val) => val.in_language(),
            DefinitionOrSubtype::ClassDefinition(val) => val.in_language(),
        }
    }
    fn see_also<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        match self {
            DefinitionOrSubtype::EnumDefinition(val) => val.see_also().map(|x| x.to_any()),
            DefinitionOrSubtype::SlotDefinition(val) => val.see_also().map(|x| x.to_any()),
            DefinitionOrSubtype::ClassDefinition(val) => val.see_also().map(|x| x.to_any()),
        }
    }
    fn deprecated_element_has_exact_replacement<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        match self {
            DefinitionOrSubtype::EnumDefinition(val) => {
                val.deprecated_element_has_exact_replacement()
            }
            DefinitionOrSubtype::SlotDefinition(val) => {
                val.deprecated_element_has_exact_replacement()
            }
            DefinitionOrSubtype::ClassDefinition(val) => {
                val.deprecated_element_has_exact_replacement()
            }
        }
    }
    fn deprecated_element_has_possible_replacement<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        match self {
            DefinitionOrSubtype::EnumDefinition(val) => {
                val.deprecated_element_has_possible_replacement()
            }
            DefinitionOrSubtype::SlotDefinition(val) => {
                val.deprecated_element_has_possible_replacement()
            }
            DefinitionOrSubtype::ClassDefinition(val) => {
                val.deprecated_element_has_possible_replacement()
            }
        }
    }
    fn aliases<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        match self {
            DefinitionOrSubtype::EnumDefinition(val) => val.aliases().map(|x| x.to_any()),
            DefinitionOrSubtype::SlotDefinition(val) => val.aliases().map(|x| x.to_any()),
            DefinitionOrSubtype::ClassDefinition(val) => val.aliases().map(|x| x.to_any()),
        }
    }
    fn structured_aliases<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::StructuredAlias>> {
        match self {
            DefinitionOrSubtype::EnumDefinition(val) => {
                val.structured_aliases().map(|x| x.to_any())
            }
            DefinitionOrSubtype::SlotDefinition(val) => {
                val.structured_aliases().map(|x| x.to_any())
            }
            DefinitionOrSubtype::ClassDefinition(val) => {
                val.structured_aliases().map(|x| x.to_any())
            }
        }
    }
    fn mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        match self {
            DefinitionOrSubtype::EnumDefinition(val) => val.mappings().map(|x| x.to_any()),
            DefinitionOrSubtype::SlotDefinition(val) => val.mappings().map(|x| x.to_any()),
            DefinitionOrSubtype::ClassDefinition(val) => val.mappings().map(|x| x.to_any()),
        }
    }
    fn exact_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        match self {
            DefinitionOrSubtype::EnumDefinition(val) => val.exact_mappings().map(|x| x.to_any()),
            DefinitionOrSubtype::SlotDefinition(val) => val.exact_mappings().map(|x| x.to_any()),
            DefinitionOrSubtype::ClassDefinition(val) => val.exact_mappings().map(|x| x.to_any()),
        }
    }
    fn close_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        match self {
            DefinitionOrSubtype::EnumDefinition(val) => val.close_mappings().map(|x| x.to_any()),
            DefinitionOrSubtype::SlotDefinition(val) => val.close_mappings().map(|x| x.to_any()),
            DefinitionOrSubtype::ClassDefinition(val) => val.close_mappings().map(|x| x.to_any()),
        }
    }
    fn related_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        match self {
            DefinitionOrSubtype::EnumDefinition(val) => val.related_mappings().map(|x| x.to_any()),
            DefinitionOrSubtype::SlotDefinition(val) => val.related_mappings().map(|x| x.to_any()),
            DefinitionOrSubtype::ClassDefinition(val) => val.related_mappings().map(|x| x.to_any()),
        }
    }
    fn narrow_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        match self {
            DefinitionOrSubtype::EnumDefinition(val) => val.narrow_mappings().map(|x| x.to_any()),
            DefinitionOrSubtype::SlotDefinition(val) => val.narrow_mappings().map(|x| x.to_any()),
            DefinitionOrSubtype::ClassDefinition(val) => val.narrow_mappings().map(|x| x.to_any()),
        }
    }
    fn broad_mappings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        match self {
            DefinitionOrSubtype::EnumDefinition(val) => val.broad_mappings().map(|x| x.to_any()),
            DefinitionOrSubtype::SlotDefinition(val) => val.broad_mappings().map(|x| x.to_any()),
            DefinitionOrSubtype::ClassDefinition(val) => val.broad_mappings().map(|x| x.to_any()),
        }
    }
    fn created_by<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        match self {
            DefinitionOrSubtype::EnumDefinition(val) => val.created_by(),
            DefinitionOrSubtype::SlotDefinition(val) => val.created_by(),
            DefinitionOrSubtype::ClassDefinition(val) => val.created_by(),
        }
    }
    fn contributors<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        match self {
            DefinitionOrSubtype::EnumDefinition(val) => val.contributors().map(|x| x.to_any()),
            DefinitionOrSubtype::SlotDefinition(val) => val.contributors().map(|x| x.to_any()),
            DefinitionOrSubtype::ClassDefinition(val) => val.contributors().map(|x| x.to_any()),
        }
    }
    fn created_on<'a>(&'a self) -> Option<&'a crate::NaiveDateTime> {
        match self {
            DefinitionOrSubtype::EnumDefinition(val) => val.created_on(),
            DefinitionOrSubtype::SlotDefinition(val) => val.created_on(),
            DefinitionOrSubtype::ClassDefinition(val) => val.created_on(),
        }
    }
    fn last_updated_on<'a>(&'a self) -> Option<&'a crate::NaiveDateTime> {
        match self {
            DefinitionOrSubtype::EnumDefinition(val) => val.last_updated_on(),
            DefinitionOrSubtype::SlotDefinition(val) => val.last_updated_on(),
            DefinitionOrSubtype::ClassDefinition(val) => val.last_updated_on(),
        }
    }
    fn modified_by<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        match self {
            DefinitionOrSubtype::EnumDefinition(val) => val.modified_by(),
            DefinitionOrSubtype::SlotDefinition(val) => val.modified_by(),
            DefinitionOrSubtype::ClassDefinition(val) => val.modified_by(),
        }
    }
    fn status<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        match self {
            DefinitionOrSubtype::EnumDefinition(val) => val.status(),
            DefinitionOrSubtype::SlotDefinition(val) => val.status(),
            DefinitionOrSubtype::ClassDefinition(val) => val.status(),
        }
    }
    fn rank(&self) -> Option<isize> {
        match self {
            DefinitionOrSubtype::EnumDefinition(val) => val.rank(),
            DefinitionOrSubtype::SlotDefinition(val) => val.rank(),
            DefinitionOrSubtype::ClassDefinition(val) => val.rank(),
        }
    }
    fn categories<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        match self {
            DefinitionOrSubtype::EnumDefinition(val) => val.categories().map(|x| x.to_any()),
            DefinitionOrSubtype::SlotDefinition(val) => val.categories().map(|x| x.to_any()),
            DefinitionOrSubtype::ClassDefinition(val) => val.categories().map(|x| x.to_any()),
        }
    }
    fn keywords<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        match self {
            DefinitionOrSubtype::EnumDefinition(val) => val.keywords().map(|x| x.to_any()),
            DefinitionOrSubtype::SlotDefinition(val) => val.keywords().map(|x| x.to_any()),
            DefinitionOrSubtype::ClassDefinition(val) => val.keywords().map(|x| x.to_any()),
        }
    }
}

pub trait Element: Extensible + Annotatable + CommonMetadata {
    fn name(&self) -> element_utl::name_range;
    // fn name_mut(&mut self) -> &mut element_utl::name_range;
    // fn set_name(&mut self, value: String);

    fn id_prefixes<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::ncname>>;
    // fn id_prefixes_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, crate::ncname>>;
    // fn set_id_prefixes(&mut self, value: Option<&Vec<ncname>>);

    fn id_prefixes_are_closed(&self) -> Option<bool>;
    // fn id_prefixes_are_closed_mut(&mut self) -> &mut Option<bool>;
    // fn set_id_prefixes_are_closed(&mut self, value: Option<bool>);

    fn definition_uri<'a>(&'a self) -> Option<&'a crate::uriorcurie>;
    // fn definition_uri_mut(&mut self) -> &mut Option<&'a crate::uriorcurie>;
    // fn set_definition_uri(&mut self, value: Option<&'a uriorcurie>);

    fn local_names<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::LocalName>>;
    // fn local_names_mut(&mut self) -> &mut Option<impl poly_containers::MapRef<'a, String,crate::LocalName>>;
    // fn set_local_names<E>(&mut self, value: Option<&HashMap<String, E>>) where E: Into<LocalName>;

    fn conforms_to<'a>(&'a self) -> Option<&'a str>;
    // fn conforms_to_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_conforms_to(&mut self, value: Option<&'a str>);

    fn implements<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>>;
    // fn implements_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>>;
    // fn set_implements(&mut self, value: Option<&Vec<uriorcurie>>);

    fn instantiates<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>>;
    // fn instantiates_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>>;
    // fn set_instantiates(&mut self, value: Option<&Vec<uriorcurie>>);
}

impl Element for crate::Element {
    fn name(&self) -> element_utl::name_range {
        element_utl::name_range::String(self.name.clone())
    }
    fn id_prefixes<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::ncname>> {
        return self.id_prefixes.as_ref();
    }
    fn id_prefixes_are_closed(&self) -> Option<bool> {
        return self.id_prefixes_are_closed;
    }
    fn definition_uri<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.definition_uri.as_ref();
    }
    fn local_names<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::LocalName>> {
        return self.local_names.as_ref();
    }
    fn conforms_to<'a>(&'a self) -> Option<&'a str> {
        return self.conforms_to.as_deref();
    }
    fn implements<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.implements.as_ref();
    }
    fn instantiates<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.instantiates.as_ref();
    }
}
impl Element for crate::SchemaDefinition {
    fn name(&self) -> element_utl::name_range {
        element_utl::name_range::ncname(self.name.clone())
    }
    fn id_prefixes<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::ncname>> {
        return self.id_prefixes.as_ref();
    }
    fn id_prefixes_are_closed(&self) -> Option<bool> {
        return self.id_prefixes_are_closed;
    }
    fn definition_uri<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.definition_uri.as_ref();
    }
    fn local_names<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::LocalName>> {
        return self.local_names.as_ref();
    }
    fn conforms_to<'a>(&'a self) -> Option<&'a str> {
        return self.conforms_to.as_deref();
    }
    fn implements<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.implements.as_ref();
    }
    fn instantiates<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.instantiates.as_ref();
    }
}
impl Element for crate::TypeDefinition {
    fn name(&self) -> element_utl::name_range {
        element_utl::name_range::String(self.name.clone())
    }
    fn id_prefixes<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::ncname>> {
        return self.id_prefixes.as_ref();
    }
    fn id_prefixes_are_closed(&self) -> Option<bool> {
        return self.id_prefixes_are_closed;
    }
    fn definition_uri<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.definition_uri.as_ref();
    }
    fn local_names<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::LocalName>> {
        return self.local_names.as_ref();
    }
    fn conforms_to<'a>(&'a self) -> Option<&'a str> {
        return self.conforms_to.as_deref();
    }
    fn implements<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.implements.as_ref();
    }
    fn instantiates<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.instantiates.as_ref();
    }
}
impl Element for crate::SubsetDefinition {
    fn name(&self) -> element_utl::name_range {
        element_utl::name_range::String(self.name.clone())
    }
    fn id_prefixes<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::ncname>> {
        return self.id_prefixes.as_ref();
    }
    fn id_prefixes_are_closed(&self) -> Option<bool> {
        return self.id_prefixes_are_closed;
    }
    fn definition_uri<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.definition_uri.as_ref();
    }
    fn local_names<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::LocalName>> {
        return self.local_names.as_ref();
    }
    fn conforms_to<'a>(&'a self) -> Option<&'a str> {
        return self.conforms_to.as_deref();
    }
    fn implements<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.implements.as_ref();
    }
    fn instantiates<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.instantiates.as_ref();
    }
}
impl Element for crate::Definition {
    fn name(&self) -> element_utl::name_range {
        element_utl::name_range::String(self.name.clone())
    }
    fn id_prefixes<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::ncname>> {
        return self.id_prefixes.as_ref();
    }
    fn id_prefixes_are_closed(&self) -> Option<bool> {
        return self.id_prefixes_are_closed;
    }
    fn definition_uri<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.definition_uri.as_ref();
    }
    fn local_names<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::LocalName>> {
        return self.local_names.as_ref();
    }
    fn conforms_to<'a>(&'a self) -> Option<&'a str> {
        return self.conforms_to.as_deref();
    }
    fn implements<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.implements.as_ref();
    }
    fn instantiates<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.instantiates.as_ref();
    }
}
impl Element for crate::EnumDefinition {
    fn name(&self) -> element_utl::name_range {
        element_utl::name_range::String(self.name.clone())
    }
    fn id_prefixes<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::ncname>> {
        return self.id_prefixes.as_ref();
    }
    fn id_prefixes_are_closed(&self) -> Option<bool> {
        return self.id_prefixes_are_closed;
    }
    fn definition_uri<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.definition_uri.as_ref();
    }
    fn local_names<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::LocalName>> {
        return self.local_names.as_ref();
    }
    fn conforms_to<'a>(&'a self) -> Option<&'a str> {
        return self.conforms_to.as_deref();
    }
    fn implements<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.implements.as_ref();
    }
    fn instantiates<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.instantiates.as_ref();
    }
}
impl Element for crate::SlotDefinition {
    fn name(&self) -> element_utl::name_range {
        element_utl::name_range::String(self.name.clone())
    }
    fn id_prefixes<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::ncname>> {
        return self.id_prefixes.as_ref();
    }
    fn id_prefixes_are_closed(&self) -> Option<bool> {
        return self.id_prefixes_are_closed;
    }
    fn definition_uri<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.definition_uri.as_ref();
    }
    fn local_names<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::LocalName>> {
        return self.local_names.as_ref();
    }
    fn conforms_to<'a>(&'a self) -> Option<&'a str> {
        return self.conforms_to.as_deref();
    }
    fn implements<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.implements.as_ref();
    }
    fn instantiates<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.instantiates.as_ref();
    }
}
impl Element for crate::ClassDefinition {
    fn name(&self) -> element_utl::name_range {
        element_utl::name_range::String(self.name.clone())
    }
    fn id_prefixes<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::ncname>> {
        return self.id_prefixes.as_ref();
    }
    fn id_prefixes_are_closed(&self) -> Option<bool> {
        return self.id_prefixes_are_closed;
    }
    fn definition_uri<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.definition_uri.as_ref();
    }
    fn local_names<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::LocalName>> {
        return self.local_names.as_ref();
    }
    fn conforms_to<'a>(&'a self) -> Option<&'a str> {
        return self.conforms_to.as_deref();
    }
    fn implements<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.implements.as_ref();
    }
    fn instantiates<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.instantiates.as_ref();
    }
}

impl Element for crate::ElementOrSubtype {
    fn name(&self) -> element_utl::name_range {
        match self {
            ElementOrSubtype::SchemaDefinition(val) => val.name(),
            ElementOrSubtype::TypeDefinition(val) => val.name(),
            ElementOrSubtype::SubsetDefinition(val) => val.name(),
            ElementOrSubtype::Definition(val) => val.name(),
            ElementOrSubtype::EnumDefinition(val) => val.name(),
            ElementOrSubtype::SlotDefinition(val) => val.name(),
            ElementOrSubtype::ClassDefinition(val) => val.name(),
        }
    }
    fn id_prefixes<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::ncname>> {
        match self {
            ElementOrSubtype::SchemaDefinition(val) => val.id_prefixes().map(|x| x.to_any()),
            ElementOrSubtype::TypeDefinition(val) => val.id_prefixes().map(|x| x.to_any()),
            ElementOrSubtype::SubsetDefinition(val) => val.id_prefixes().map(|x| x.to_any()),
            ElementOrSubtype::Definition(val) => val.id_prefixes().map(|x| x.to_any()),
            ElementOrSubtype::EnumDefinition(val) => val.id_prefixes().map(|x| x.to_any()),
            ElementOrSubtype::SlotDefinition(val) => val.id_prefixes().map(|x| x.to_any()),
            ElementOrSubtype::ClassDefinition(val) => val.id_prefixes().map(|x| x.to_any()),
        }
    }
    fn id_prefixes_are_closed(&self) -> Option<bool> {
        match self {
            ElementOrSubtype::SchemaDefinition(val) => val.id_prefixes_are_closed(),
            ElementOrSubtype::TypeDefinition(val) => val.id_prefixes_are_closed(),
            ElementOrSubtype::SubsetDefinition(val) => val.id_prefixes_are_closed(),
            ElementOrSubtype::Definition(val) => val.id_prefixes_are_closed(),
            ElementOrSubtype::EnumDefinition(val) => val.id_prefixes_are_closed(),
            ElementOrSubtype::SlotDefinition(val) => val.id_prefixes_are_closed(),
            ElementOrSubtype::ClassDefinition(val) => val.id_prefixes_are_closed(),
        }
    }
    fn definition_uri<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        match self {
            ElementOrSubtype::SchemaDefinition(val) => val.definition_uri(),
            ElementOrSubtype::TypeDefinition(val) => val.definition_uri(),
            ElementOrSubtype::SubsetDefinition(val) => val.definition_uri(),
            ElementOrSubtype::Definition(val) => val.definition_uri(),
            ElementOrSubtype::EnumDefinition(val) => val.definition_uri(),
            ElementOrSubtype::SlotDefinition(val) => val.definition_uri(),
            ElementOrSubtype::ClassDefinition(val) => val.definition_uri(),
        }
    }
    fn local_names<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::LocalName>> {
        match self {
            ElementOrSubtype::SchemaDefinition(val) => val.local_names().map(|x| x.to_any()),
            ElementOrSubtype::TypeDefinition(val) => val.local_names().map(|x| x.to_any()),
            ElementOrSubtype::SubsetDefinition(val) => val.local_names().map(|x| x.to_any()),
            ElementOrSubtype::Definition(val) => val.local_names().map(|x| x.to_any()),
            ElementOrSubtype::EnumDefinition(val) => val.local_names().map(|x| x.to_any()),
            ElementOrSubtype::SlotDefinition(val) => val.local_names().map(|x| x.to_any()),
            ElementOrSubtype::ClassDefinition(val) => val.local_names().map(|x| x.to_any()),
        }
    }
    fn conforms_to<'a>(&'a self) -> Option<&'a str> {
        match self {
            ElementOrSubtype::SchemaDefinition(val) => val.conforms_to(),
            ElementOrSubtype::TypeDefinition(val) => val.conforms_to(),
            ElementOrSubtype::SubsetDefinition(val) => val.conforms_to(),
            ElementOrSubtype::Definition(val) => val.conforms_to(),
            ElementOrSubtype::EnumDefinition(val) => val.conforms_to(),
            ElementOrSubtype::SlotDefinition(val) => val.conforms_to(),
            ElementOrSubtype::ClassDefinition(val) => val.conforms_to(),
        }
    }
    fn implements<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        match self {
            ElementOrSubtype::SchemaDefinition(val) => val.implements().map(|x| x.to_any()),
            ElementOrSubtype::TypeDefinition(val) => val.implements().map(|x| x.to_any()),
            ElementOrSubtype::SubsetDefinition(val) => val.implements().map(|x| x.to_any()),
            ElementOrSubtype::Definition(val) => val.implements().map(|x| x.to_any()),
            ElementOrSubtype::EnumDefinition(val) => val.implements().map(|x| x.to_any()),
            ElementOrSubtype::SlotDefinition(val) => val.implements().map(|x| x.to_any()),
            ElementOrSubtype::ClassDefinition(val) => val.implements().map(|x| x.to_any()),
        }
    }
    fn instantiates<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        match self {
            ElementOrSubtype::SchemaDefinition(val) => val.instantiates().map(|x| x.to_any()),
            ElementOrSubtype::TypeDefinition(val) => val.instantiates().map(|x| x.to_any()),
            ElementOrSubtype::SubsetDefinition(val) => val.instantiates().map(|x| x.to_any()),
            ElementOrSubtype::Definition(val) => val.instantiates().map(|x| x.to_any()),
            ElementOrSubtype::EnumDefinition(val) => val.instantiates().map(|x| x.to_any()),
            ElementOrSubtype::SlotDefinition(val) => val.instantiates().map(|x| x.to_any()),
            ElementOrSubtype::ClassDefinition(val) => val.instantiates().map(|x| x.to_any()),
        }
    }
}
impl Element for crate::DefinitionOrSubtype {
    fn name(&self) -> element_utl::name_range {
        match self {
            DefinitionOrSubtype::EnumDefinition(val) => val.name(),
            DefinitionOrSubtype::SlotDefinition(val) => val.name(),
            DefinitionOrSubtype::ClassDefinition(val) => val.name(),
        }
    }
    fn id_prefixes<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::ncname>> {
        match self {
            DefinitionOrSubtype::EnumDefinition(val) => val.id_prefixes().map(|x| x.to_any()),
            DefinitionOrSubtype::SlotDefinition(val) => val.id_prefixes().map(|x| x.to_any()),
            DefinitionOrSubtype::ClassDefinition(val) => val.id_prefixes().map(|x| x.to_any()),
        }
    }
    fn id_prefixes_are_closed(&self) -> Option<bool> {
        match self {
            DefinitionOrSubtype::EnumDefinition(val) => val.id_prefixes_are_closed(),
            DefinitionOrSubtype::SlotDefinition(val) => val.id_prefixes_are_closed(),
            DefinitionOrSubtype::ClassDefinition(val) => val.id_prefixes_are_closed(),
        }
    }
    fn definition_uri<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        match self {
            DefinitionOrSubtype::EnumDefinition(val) => val.definition_uri(),
            DefinitionOrSubtype::SlotDefinition(val) => val.definition_uri(),
            DefinitionOrSubtype::ClassDefinition(val) => val.definition_uri(),
        }
    }
    fn local_names<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::LocalName>> {
        match self {
            DefinitionOrSubtype::EnumDefinition(val) => val.local_names().map(|x| x.to_any()),
            DefinitionOrSubtype::SlotDefinition(val) => val.local_names().map(|x| x.to_any()),
            DefinitionOrSubtype::ClassDefinition(val) => val.local_names().map(|x| x.to_any()),
        }
    }
    fn conforms_to<'a>(&'a self) -> Option<&'a str> {
        match self {
            DefinitionOrSubtype::EnumDefinition(val) => val.conforms_to(),
            DefinitionOrSubtype::SlotDefinition(val) => val.conforms_to(),
            DefinitionOrSubtype::ClassDefinition(val) => val.conforms_to(),
        }
    }
    fn implements<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        match self {
            DefinitionOrSubtype::EnumDefinition(val) => val.implements().map(|x| x.to_any()),
            DefinitionOrSubtype::SlotDefinition(val) => val.implements().map(|x| x.to_any()),
            DefinitionOrSubtype::ClassDefinition(val) => val.implements().map(|x| x.to_any()),
        }
    }
    fn instantiates<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        match self {
            DefinitionOrSubtype::EnumDefinition(val) => val.instantiates().map(|x| x.to_any()),
            DefinitionOrSubtype::SlotDefinition(val) => val.instantiates().map(|x| x.to_any()),
            DefinitionOrSubtype::ClassDefinition(val) => val.instantiates().map(|x| x.to_any()),
        }
    }
}

pub trait SchemaDefinition: Element {
    fn id<'a>(&'a self) -> &'a crate::uri;
    // fn id_mut(&mut self) -> &mut &'a crate::uri;
    // fn set_id(&mut self, value: uri);

    fn version<'a>(&'a self) -> Option<&'a str>;
    // fn version_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_version(&mut self, value: Option<&'a str>);

    fn imports<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>>;
    // fn imports_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>>;
    // fn set_imports(&mut self, value: Option<&Vec<uriorcurie>>);

    fn license<'a>(&'a self) -> Option<&'a str>;
    // fn license_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_license(&mut self, value: Option<&'a str>);

    fn prefixes<'a>(&'a self) -> Option<impl poly_containers::MapRef<'a, String, crate::Prefix>>;
    // fn prefixes_mut(&mut self) -> &mut Option<impl poly_containers::MapRef<'a, String,crate::Prefix>>;
    // fn set_prefixes<E>(&mut self, value: Option<&HashMap<String, E>>) where E: Into<Prefix>;

    fn emit_prefixes<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::ncname>>;
    // fn emit_prefixes_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, crate::ncname>>;
    // fn set_emit_prefixes(&mut self, value: Option<&Vec<ncname>>);

    fn default_curi_maps<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>>;
    // fn default_curi_maps_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, String>>;
    // fn set_default_curi_maps(&mut self, value: Option<&Vec<String>>);

    fn default_prefix<'a>(&'a self) -> Option<&'a str>;
    // fn default_prefix_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_default_prefix(&mut self, value: Option<&'a str>);

    fn default_range<'a>(&'a self) -> Option<&'a str>;
    // fn default_range_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_default_range<E>(&mut self, value: Option<&'a str>) where E: Into<String>;

    fn subsets<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::SubsetDefinition>>;
    // fn subsets_mut(&mut self) -> &mut Option<impl poly_containers::MapRef<'a, String,crate::SubsetDefinition>>;
    // fn set_subsets<E>(&mut self, value: Option<&HashMap<String, E>>) where E: Into<SubsetDefinition>;

    fn types<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::TypeDefinition>>;
    // fn types_mut(&mut self) -> &mut Option<impl poly_containers::MapRef<'a, String,crate::TypeDefinition>>;
    // fn set_types<E>(&mut self, value: Option<&HashMap<String, E>>) where E: Into<TypeDefinition>;

    fn enums<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::EnumDefinition>>;
    // fn enums_mut(&mut self) -> &mut Option<impl poly_containers::MapRef<'a, String,crate::EnumDefinition>>;
    // fn set_enums<E>(&mut self, value: Option<&HashMap<String, E>>) where E: Into<EnumDefinition>;

    fn slot_definitions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::SlotDefinition>>;
    // fn slot_definitions_mut(&mut self) -> &mut Option<impl poly_containers::MapRef<'a, String,crate::SlotDefinition>>;
    // fn set_slot_definitions<E>(&mut self, value: Option<&HashMap<String, E>>) where E: Into<SlotDefinition>;

    fn classes<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::ClassDefinition>>;
    // fn classes_mut(&mut self) -> &mut Option<impl poly_containers::MapRef<'a, String,crate::ClassDefinition>>;
    // fn set_classes<E>(&mut self, value: Option<&HashMap<String, E>>) where E: Into<ClassDefinition>;

    fn metamodel_version<'a>(&'a self) -> Option<&'a str>;
    // fn metamodel_version_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_metamodel_version(&mut self, value: Option<&'a str>);

    fn source_file<'a>(&'a self) -> Option<&'a str>;
    // fn source_file_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_source_file(&mut self, value: Option<&'a str>);

    fn source_file_date<'a>(&'a self) -> Option<&'a crate::NaiveDateTime>;
    // fn source_file_date_mut(&mut self) -> &mut Option<&'a crate::NaiveDateTime>;
    // fn set_source_file_date(&mut self, value: Option<&'a NaiveDateTime>);

    fn source_file_size(&self) -> Option<isize>;
    // fn source_file_size_mut(&mut self) -> &mut Option<isize>;
    // fn set_source_file_size(&mut self, value: Option<isize>);

    fn generation_date<'a>(&'a self) -> Option<&'a crate::NaiveDateTime>;
    // fn generation_date_mut(&mut self) -> &mut Option<&'a crate::NaiveDateTime>;
    // fn set_generation_date(&mut self, value: Option<&'a NaiveDateTime>);

    fn slot_names_unique(&self) -> Option<bool>;
    // fn slot_names_unique_mut(&mut self) -> &mut Option<bool>;
    // fn set_slot_names_unique(&mut self, value: Option<bool>);

    fn settings<'a>(&'a self) -> Option<impl poly_containers::MapRef<'a, String, crate::Setting>>;
    // fn settings_mut(&mut self) -> &mut Option<impl poly_containers::MapRef<'a, String,crate::Setting>>;
    // fn set_settings<E>(&mut self, value: Option<&HashMap<String, E>>) where E: Into<Setting>;

    fn bindings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::EnumBinding>>;
    // fn bindings_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, crate::EnumBinding>>;
    // fn set_bindings<E>(&mut self, value: Option<&Vec<E>>) where E: Into<EnumBinding>;
}

impl SchemaDefinition for crate::SchemaDefinition {
    fn id<'a>(&'a self) -> &'a crate::uri {
        return &self.id;
    }
    fn version<'a>(&'a self) -> Option<&'a str> {
        return self.version.as_deref();
    }
    fn imports<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.imports.as_ref();
    }
    fn license<'a>(&'a self) -> Option<&'a str> {
        return self.license.as_deref();
    }
    fn prefixes<'a>(&'a self) -> Option<impl poly_containers::MapRef<'a, String, crate::Prefix>> {
        return self.prefixes.as_ref();
    }
    fn emit_prefixes<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::ncname>> {
        return self.emit_prefixes.as_ref();
    }
    fn default_curi_maps<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.default_curi_maps.as_ref();
    }
    fn default_prefix<'a>(&'a self) -> Option<&'a str> {
        return self.default_prefix.as_deref();
    }
    fn default_range<'a>(&'a self) -> Option<&'a str> {
        return self.default_range.as_deref();
    }
    fn subsets<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::SubsetDefinition>> {
        return self.subsets.as_ref();
    }
    fn types<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::TypeDefinition>> {
        return self.types.as_ref();
    }
    fn enums<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::EnumDefinition>> {
        return self.enums.as_ref();
    }
    fn slot_definitions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::SlotDefinition>> {
        return self.slot_definitions.as_ref();
    }
    fn classes<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::ClassDefinition>> {
        return self.classes.as_ref();
    }
    fn metamodel_version<'a>(&'a self) -> Option<&'a str> {
        return self.metamodel_version.as_deref();
    }
    fn source_file<'a>(&'a self) -> Option<&'a str> {
        return self.source_file.as_deref();
    }
    fn source_file_date<'a>(&'a self) -> Option<&'a crate::NaiveDateTime> {
        return self.source_file_date.as_ref();
    }
    fn source_file_size(&self) -> Option<isize> {
        return self.source_file_size;
    }
    fn generation_date<'a>(&'a self) -> Option<&'a crate::NaiveDateTime> {
        return self.generation_date.as_ref();
    }
    fn slot_names_unique(&self) -> Option<bool> {
        return self.slot_names_unique;
    }
    fn settings<'a>(&'a self) -> Option<impl poly_containers::MapRef<'a, String, crate::Setting>> {
        return self.settings.as_ref();
    }
    fn bindings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::EnumBinding>> {
        return self.bindings.as_ref();
    }
}

pub trait AnonymousTypeExpression: TypeExpression {}

impl AnonymousTypeExpression for crate::AnonymousTypeExpression {}

pub trait TypeDefinition: Element + TypeExpression {
    fn typeof_<'a>(&'a self) -> Option<&'a str>;
    // fn typeof__mut(&mut self) -> &mut Option<&'a str>;
    // fn set_typeof_<E>(&mut self, value: Option<&'a str>) where E: Into<String>;

    fn base<'a>(&'a self) -> Option<&'a str>;
    // fn base_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_base(&mut self, value: Option<&'a str>);

    fn type_uri<'a>(&'a self) -> Option<&'a crate::uriorcurie>;
    // fn type_uri_mut(&mut self) -> &mut Option<&'a crate::uriorcurie>;
    // fn set_type_uri(&mut self, value: Option<&'a uriorcurie>);

    fn repr<'a>(&'a self) -> Option<&'a str>;
    // fn repr_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_repr(&mut self, value: Option<&'a str>);

    fn union_of<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>>;
    // fn union_of_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, String>>;
    // fn set_union_of<E>(&mut self, value: Option<&Vec<String>>) where E: Into<String>;
}

impl TypeDefinition for crate::TypeDefinition {
    fn typeof_<'a>(&'a self) -> Option<&'a str> {
        return self.typeof_.as_deref();
    }
    fn base<'a>(&'a self) -> Option<&'a str> {
        return self.base.as_deref();
    }
    fn type_uri<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.type_uri.as_ref();
    }
    fn repr<'a>(&'a self) -> Option<&'a str> {
        return self.repr.as_deref();
    }
    fn union_of<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.union_of.as_ref();
    }
}

pub trait SubsetDefinition: Element {}

impl SubsetDefinition for crate::SubsetDefinition {}

pub trait Definition: Element {
    fn is_a<'a>(&'a self) -> Option<&'a str>;
    // fn is_a_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_is_a<E>(&mut self, value: Option<&'a str>) where E: Into<String>;

    fn abstract_(&self) -> Option<bool>;
    // fn abstract__mut(&mut self) -> &mut Option<bool>;
    // fn set_abstract_(&mut self, value: Option<bool>);

    fn mixin(&self) -> Option<bool>;
    // fn mixin_mut(&mut self) -> &mut Option<bool>;
    // fn set_mixin(&mut self, value: Option<bool>);

    fn mixins<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>>;
    // fn mixins_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, String>>;
    // fn set_mixins<E>(&mut self, value: Option<&Vec<String>>) where E: Into<String>;

    fn apply_to<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>>;
    // fn apply_to_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, String>>;
    // fn set_apply_to<E>(&mut self, value: Option<&Vec<String>>) where E: Into<String>;

    fn values_from<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>>;
    // fn values_from_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>>;
    // fn set_values_from(&mut self, value: Option<&Vec<uriorcurie>>);

    fn string_serialization<'a>(&'a self) -> Option<&'a str>;
    // fn string_serialization_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_string_serialization(&mut self, value: Option<&'a str>);
}

impl Definition for crate::Definition {
    fn is_a<'a>(&'a self) -> Option<&'a str> {
        return self.is_a.as_deref();
    }
    fn abstract_(&self) -> Option<bool> {
        return self.abstract_;
    }
    fn mixin(&self) -> Option<bool> {
        return self.mixin;
    }
    fn mixins<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.mixins.as_ref();
    }
    fn apply_to<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.apply_to.as_ref();
    }
    fn values_from<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.values_from.as_ref();
    }
    fn string_serialization<'a>(&'a self) -> Option<&'a str> {
        return self.string_serialization.as_deref();
    }
}
impl Definition for crate::EnumDefinition {
    fn is_a<'a>(&'a self) -> Option<&'a str> {
        return self.is_a.as_deref();
    }
    fn abstract_(&self) -> Option<bool> {
        return self.abstract_;
    }
    fn mixin(&self) -> Option<bool> {
        return self.mixin;
    }
    fn mixins<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.mixins.as_ref();
    }
    fn apply_to<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.apply_to.as_ref();
    }
    fn values_from<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.values_from.as_ref();
    }
    fn string_serialization<'a>(&'a self) -> Option<&'a str> {
        return self.string_serialization.as_deref();
    }
}
impl Definition for crate::SlotDefinition {
    fn is_a<'a>(&'a self) -> Option<&'a str> {
        return self.is_a.as_deref();
    }
    fn abstract_(&self) -> Option<bool> {
        return self.abstract_;
    }
    fn mixin(&self) -> Option<bool> {
        return self.mixin;
    }
    fn mixins<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.mixins.as_ref();
    }
    fn apply_to<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.apply_to.as_ref();
    }
    fn values_from<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.values_from.as_ref();
    }
    fn string_serialization<'a>(&'a self) -> Option<&'a str> {
        return self.string_serialization.as_deref();
    }
}
impl Definition for crate::ClassDefinition {
    fn is_a<'a>(&'a self) -> Option<&'a str> {
        return self.is_a.as_deref();
    }
    fn abstract_(&self) -> Option<bool> {
        return self.abstract_;
    }
    fn mixin(&self) -> Option<bool> {
        return self.mixin;
    }
    fn mixins<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.mixins.as_ref();
    }
    fn apply_to<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.apply_to.as_ref();
    }
    fn values_from<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.values_from.as_ref();
    }
    fn string_serialization<'a>(&'a self) -> Option<&'a str> {
        return self.string_serialization.as_deref();
    }
}

impl Definition for crate::DefinitionOrSubtype {
    fn is_a<'a>(&'a self) -> Option<&'a str> {
        match self {
            DefinitionOrSubtype::EnumDefinition(val) => val.is_a(),
            DefinitionOrSubtype::SlotDefinition(val) => val.is_a(),
            DefinitionOrSubtype::ClassDefinition(val) => val.is_a(),
        }
    }
    fn abstract_(&self) -> Option<bool> {
        match self {
            DefinitionOrSubtype::EnumDefinition(val) => val.abstract_(),
            DefinitionOrSubtype::SlotDefinition(val) => val.abstract_(),
            DefinitionOrSubtype::ClassDefinition(val) => val.abstract_(),
        }
    }
    fn mixin(&self) -> Option<bool> {
        match self {
            DefinitionOrSubtype::EnumDefinition(val) => val.mixin(),
            DefinitionOrSubtype::SlotDefinition(val) => val.mixin(),
            DefinitionOrSubtype::ClassDefinition(val) => val.mixin(),
        }
    }
    fn mixins<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        match self {
            DefinitionOrSubtype::EnumDefinition(val) => val.mixins().map(|x| x.to_any()),
            DefinitionOrSubtype::SlotDefinition(val) => val.mixins().map(|x| x.to_any()),
            DefinitionOrSubtype::ClassDefinition(val) => val.mixins().map(|x| x.to_any()),
        }
    }
    fn apply_to<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        match self {
            DefinitionOrSubtype::EnumDefinition(val) => val.apply_to().map(|x| x.to_any()),
            DefinitionOrSubtype::SlotDefinition(val) => val.apply_to().map(|x| x.to_any()),
            DefinitionOrSubtype::ClassDefinition(val) => val.apply_to().map(|x| x.to_any()),
        }
    }
    fn values_from<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        match self {
            DefinitionOrSubtype::EnumDefinition(val) => val.values_from().map(|x| x.to_any()),
            DefinitionOrSubtype::SlotDefinition(val) => val.values_from().map(|x| x.to_any()),
            DefinitionOrSubtype::ClassDefinition(val) => val.values_from().map(|x| x.to_any()),
        }
    }
    fn string_serialization<'a>(&'a self) -> Option<&'a str> {
        match self {
            DefinitionOrSubtype::EnumDefinition(val) => val.string_serialization(),
            DefinitionOrSubtype::SlotDefinition(val) => val.string_serialization(),
            DefinitionOrSubtype::ClassDefinition(val) => val.string_serialization(),
        }
    }
}

pub trait AnonymousEnumExpression: EnumExpression {}

impl AnonymousEnumExpression for crate::AnonymousEnumExpression {}

pub trait EnumDefinition: Definition + EnumExpression {
    fn enum_uri<'a>(&'a self) -> Option<&'a crate::uriorcurie>;
    // fn enum_uri_mut(&mut self) -> &mut Option<&'a crate::uriorcurie>;
    // fn set_enum_uri(&mut self, value: Option<&'a uriorcurie>);
}

impl EnumDefinition for crate::EnumDefinition {
    fn enum_uri<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.enum_uri.as_ref();
    }
}

pub trait EnumBinding: Extensible + Annotatable + CommonMetadata {
    fn range<'a>(&'a self) -> Option<&'a str>;
    // fn range_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_range<E>(&mut self, value: Option<&'a str>) where E: Into<String>;

    fn obligation_level<'a>(&'a self) -> Option<&'a crate::ObligationLevelEnum>;
    // fn obligation_level_mut(&mut self) -> &mut Option<&'a crate::ObligationLevelEnum>;
    // fn set_obligation_level(&mut self, value: Option<&'a ObligationLevelEnum>);

    fn binds_value_of<'a>(&'a self) -> Option<&'a str>;
    // fn binds_value_of_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_binds_value_of(&mut self, value: Option<&'a str>);

    fn pv_formula<'a>(&'a self) -> Option<&'a crate::PvFormulaOptions>;
    // fn pv_formula_mut(&mut self) -> &mut Option<&'a crate::PvFormulaOptions>;
    // fn set_pv_formula(&mut self, value: Option<&'a PvFormulaOptions>);
}

impl EnumBinding for crate::EnumBinding {
    fn range<'a>(&'a self) -> Option<&'a str> {
        return self.range.as_deref();
    }
    fn obligation_level<'a>(&'a self) -> Option<&'a crate::ObligationLevelEnum> {
        return self.obligation_level.as_ref();
    }
    fn binds_value_of<'a>(&'a self) -> Option<&'a str> {
        return self.binds_value_of.as_deref();
    }
    fn pv_formula<'a>(&'a self) -> Option<&'a crate::PvFormulaOptions> {
        return self.pv_formula.as_ref();
    }
}

pub trait MatchQuery {
    fn identifier_pattern<'a>(&'a self) -> Option<&'a str>;
    // fn identifier_pattern_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_identifier_pattern(&mut self, value: Option<&'a str>);

    fn source_ontology<'a>(&'a self) -> Option<&'a crate::uriorcurie>;
    // fn source_ontology_mut(&mut self) -> &mut Option<&'a crate::uriorcurie>;
    // fn set_source_ontology(&mut self, value: Option<&'a uriorcurie>);
}

impl MatchQuery for crate::MatchQuery {
    fn identifier_pattern<'a>(&'a self) -> Option<&'a str> {
        return self.identifier_pattern.as_deref();
    }
    fn source_ontology<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.source_ontology.as_ref();
    }
}

pub trait ReachabilityQuery {
    fn source_ontology<'a>(&'a self) -> Option<&'a crate::uriorcurie>;
    // fn source_ontology_mut(&mut self) -> &mut Option<&'a crate::uriorcurie>;
    // fn set_source_ontology(&mut self, value: Option<&'a uriorcurie>);

    fn source_nodes<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>>;
    // fn source_nodes_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>>;
    // fn set_source_nodes(&mut self, value: Option<&Vec<uriorcurie>>);

    fn relationship_types<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>>;
    // fn relationship_types_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>>;
    // fn set_relationship_types(&mut self, value: Option<&Vec<uriorcurie>>);

    fn is_direct(&self) -> Option<bool>;
    // fn is_direct_mut(&mut self) -> &mut Option<bool>;
    // fn set_is_direct(&mut self, value: Option<bool>);

    fn include_self(&self) -> Option<bool>;
    // fn include_self_mut(&mut self) -> &mut Option<bool>;
    // fn set_include_self(&mut self, value: Option<bool>);

    fn traverse_up(&self) -> Option<bool>;
    // fn traverse_up_mut(&mut self) -> &mut Option<bool>;
    // fn set_traverse_up(&mut self, value: Option<bool>);
}

impl ReachabilityQuery for crate::ReachabilityQuery {
    fn source_ontology<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.source_ontology.as_ref();
    }
    fn source_nodes<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.source_nodes.as_ref();
    }
    fn relationship_types<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.relationship_types.as_ref();
    }
    fn is_direct(&self) -> Option<bool> {
        return self.is_direct;
    }
    fn include_self(&self) -> Option<bool> {
        return self.include_self;
    }
    fn traverse_up(&self) -> Option<bool> {
        return self.traverse_up;
    }
}

pub trait StructuredAlias: Expression + Extensible + Annotatable + CommonMetadata {
    fn literal_form<'a>(&'a self) -> &'a str;
    // fn literal_form_mut(&mut self) -> &mut &'a str;
    // fn set_literal_form(&mut self, value: String);

    fn alias_predicate<'a>(&'a self) -> Option<&'a crate::AliasPredicateEnum>;
    // fn alias_predicate_mut(&mut self) -> &mut Option<&'a crate::AliasPredicateEnum>;
    // fn set_alias_predicate(&mut self, value: Option<&'a AliasPredicateEnum>);

    fn alias_contexts<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uri>>;
    // fn alias_contexts_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, crate::uri>>;
    // fn set_alias_contexts(&mut self, value: Option<&Vec<uri>>);
}

impl StructuredAlias for crate::StructuredAlias {
    fn literal_form<'a>(&'a self) -> &'a str {
        return &self.literal_form[..];
    }
    fn alias_predicate<'a>(&'a self) -> Option<&'a crate::AliasPredicateEnum> {
        return self.alias_predicate.as_ref();
    }
    fn alias_contexts<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uri>> {
        return self.alias_contexts.as_ref();
    }
}

pub trait Expression {}

impl Expression for crate::Expression {}
impl Expression for crate::TypeExpression {}
impl Expression for crate::EnumExpression {}
impl Expression for crate::StructuredAlias {}
impl Expression for crate::AnonymousExpression {}
impl Expression for crate::PathExpression {}
impl Expression for crate::SlotExpression {}
impl Expression for crate::ExtraSlotsExpression {}
impl Expression for crate::AnonymousSlotExpression {}
impl Expression for crate::SlotDefinition {}
impl Expression for crate::AnonymousClassExpression {}
impl Expression for crate::AnonymousEnumExpression {}
impl Expression for crate::EnumDefinition {}
impl Expression for crate::AnonymousTypeExpression {}
impl Expression for crate::TypeDefinition {}

impl Expression for crate::ExpressionOrSubtype {}
impl Expression for crate::TypeExpressionOrSubtype {}
impl Expression for crate::EnumExpressionOrSubtype {}
impl Expression for crate::AnonymousExpressionOrSubtype {}
impl Expression for crate::SlotExpressionOrSubtype {}

pub trait TypeExpression: Expression {
    fn pattern<'a>(&'a self) -> Option<&'a str>;
    // fn pattern_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_pattern(&mut self, value: Option<&'a str>);

    fn structured_pattern<'a>(&'a self) -> Option<&'a crate::PatternExpression>;
    // fn structured_pattern_mut(&mut self) -> &mut Option<&'a crate::PatternExpression>;
    // fn set_structured_pattern<E>(&mut self, value: Option<E>) where E: Into<PatternExpression>;

    fn unit<'a>(&'a self) -> Option<&'a crate::UnitOfMeasure>;
    // fn unit_mut(&mut self) -> &mut Option<&'a crate::UnitOfMeasure>;
    // fn set_unit<E>(&mut self, value: Option<E>) where E: Into<UnitOfMeasure>;

    fn implicit_prefix<'a>(&'a self) -> Option<&'a str>;
    // fn implicit_prefix_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_implicit_prefix(&mut self, value: Option<&'a str>);

    fn equals_string<'a>(&'a self) -> Option<&'a str>;
    // fn equals_string_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_equals_string(&mut self, value: Option<&'a str>);

    fn equals_string_in<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>>;
    // fn equals_string_in_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, String>>;
    // fn set_equals_string_in(&mut self, value: Option<&Vec<String>>);

    fn equals_number(&self) -> Option<isize>;
    // fn equals_number_mut(&mut self) -> &mut Option<isize>;
    // fn set_equals_number(&mut self, value: Option<isize>);

    fn minimum_value<'a>(&'a self) -> Option<&'a crate::Anything>;
    // fn minimum_value_mut(&mut self) -> &mut Option<&'a crate::Anything>;
    // fn set_minimum_value<E>(&mut self, value: Option<E>) where E: Into<Anything>;

    fn maximum_value<'a>(&'a self) -> Option<&'a crate::Anything>;
    // fn maximum_value_mut(&mut self) -> &mut Option<&'a crate::Anything>;
    // fn set_maximum_value<E>(&mut self, value: Option<E>) where E: Into<Anything>;

    fn none_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousTypeExpression>>;
    // fn none_of_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, crate::AnonymousTypeExpression>>;
    // fn set_none_of<E>(&mut self, value: Option<&Vec<E>>) where E: Into<AnonymousTypeExpression>;

    fn exactly_one_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousTypeExpression>>;
    // fn exactly_one_of_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, crate::AnonymousTypeExpression>>;
    // fn set_exactly_one_of<E>(&mut self, value: Option<&Vec<E>>) where E: Into<AnonymousTypeExpression>;

    fn any_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousTypeExpression>>;
    // fn any_of_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, crate::AnonymousTypeExpression>>;
    // fn set_any_of<E>(&mut self, value: Option<&Vec<E>>) where E: Into<AnonymousTypeExpression>;

    fn all_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousTypeExpression>>;
    // fn all_of_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, crate::AnonymousTypeExpression>>;
    // fn set_all_of<E>(&mut self, value: Option<&Vec<E>>) where E: Into<AnonymousTypeExpression>;
}

impl TypeExpression for crate::TypeExpression {
    fn pattern<'a>(&'a self) -> Option<&'a str> {
        return self.pattern.as_deref();
    }
    fn structured_pattern<'a>(&'a self) -> Option<&'a crate::PatternExpression> {
        return self.structured_pattern.as_ref();
    }
    fn unit<'a>(&'a self) -> Option<&'a crate::UnitOfMeasure> {
        return self.unit.as_ref();
    }
    fn implicit_prefix<'a>(&'a self) -> Option<&'a str> {
        return self.implicit_prefix.as_deref();
    }
    fn equals_string<'a>(&'a self) -> Option<&'a str> {
        return self.equals_string.as_deref();
    }
    fn equals_string_in<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.equals_string_in.as_ref();
    }
    fn equals_number(&self) -> Option<isize> {
        return self.equals_number;
    }
    fn minimum_value<'a>(&'a self) -> Option<&'a crate::Anything> {
        return self.minimum_value.as_ref();
    }
    fn maximum_value<'a>(&'a self) -> Option<&'a crate::Anything> {
        return self.maximum_value.as_ref();
    }
    fn none_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousTypeExpression>> {
        return self.none_of.as_ref();
    }
    fn exactly_one_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousTypeExpression>> {
        return self.exactly_one_of.as_ref();
    }
    fn any_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousTypeExpression>> {
        return self.any_of.as_ref();
    }
    fn all_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousTypeExpression>> {
        return self.all_of.as_ref();
    }
}
impl TypeExpression for crate::AnonymousTypeExpression {
    fn pattern<'a>(&'a self) -> Option<&'a str> {
        return self.pattern.as_deref();
    }
    fn structured_pattern<'a>(&'a self) -> Option<&'a crate::PatternExpression> {
        return self.structured_pattern.as_ref();
    }
    fn unit<'a>(&'a self) -> Option<&'a crate::UnitOfMeasure> {
        return self.unit.as_ref();
    }
    fn implicit_prefix<'a>(&'a self) -> Option<&'a str> {
        return self.implicit_prefix.as_deref();
    }
    fn equals_string<'a>(&'a self) -> Option<&'a str> {
        return self.equals_string.as_deref();
    }
    fn equals_string_in<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.equals_string_in.as_ref();
    }
    fn equals_number(&self) -> Option<isize> {
        return self.equals_number;
    }
    fn minimum_value<'a>(&'a self) -> Option<&'a crate::Anything> {
        return self.minimum_value.as_ref();
    }
    fn maximum_value<'a>(&'a self) -> Option<&'a crate::Anything> {
        return self.maximum_value.as_ref();
    }
    fn none_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousTypeExpression>> {
        return self
            .none_of
            .as_ref()
            .map(|x| poly_containers::ListView::new(x));
    }
    fn exactly_one_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousTypeExpression>> {
        return self
            .exactly_one_of
            .as_ref()
            .map(|x| poly_containers::ListView::new(x));
    }
    fn any_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousTypeExpression>> {
        return self
            .any_of
            .as_ref()
            .map(|x| poly_containers::ListView::new(x));
    }
    fn all_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousTypeExpression>> {
        return self
            .all_of
            .as_ref()
            .map(|x| poly_containers::ListView::new(x));
    }
}
impl TypeExpression for crate::TypeDefinition {
    fn pattern<'a>(&'a self) -> Option<&'a str> {
        return self.pattern.as_deref();
    }
    fn structured_pattern<'a>(&'a self) -> Option<&'a crate::PatternExpression> {
        return self.structured_pattern.as_ref();
    }
    fn unit<'a>(&'a self) -> Option<&'a crate::UnitOfMeasure> {
        return self.unit.as_ref();
    }
    fn implicit_prefix<'a>(&'a self) -> Option<&'a str> {
        return self.implicit_prefix.as_deref();
    }
    fn equals_string<'a>(&'a self) -> Option<&'a str> {
        return self.equals_string.as_deref();
    }
    fn equals_string_in<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.equals_string_in.as_ref();
    }
    fn equals_number(&self) -> Option<isize> {
        return self.equals_number;
    }
    fn minimum_value<'a>(&'a self) -> Option<&'a crate::Anything> {
        return self.minimum_value.as_ref();
    }
    fn maximum_value<'a>(&'a self) -> Option<&'a crate::Anything> {
        return self.maximum_value.as_ref();
    }
    fn none_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousTypeExpression>> {
        return self.none_of.as_ref();
    }
    fn exactly_one_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousTypeExpression>> {
        return self.exactly_one_of.as_ref();
    }
    fn any_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousTypeExpression>> {
        return self.any_of.as_ref();
    }
    fn all_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousTypeExpression>> {
        return self.all_of.as_ref();
    }
}

impl TypeExpression for crate::TypeExpressionOrSubtype {
    fn pattern<'a>(&'a self) -> Option<&'a str> {
        match self {
            TypeExpressionOrSubtype::AnonymousTypeExpression(val) => val.pattern(),
            TypeExpressionOrSubtype::TypeDefinition(val) => val.pattern(),
        }
    }
    fn structured_pattern<'a>(&'a self) -> Option<&'a crate::PatternExpression> {
        match self {
            TypeExpressionOrSubtype::AnonymousTypeExpression(val) => val.structured_pattern(),
            TypeExpressionOrSubtype::TypeDefinition(val) => val.structured_pattern(),
        }
    }
    fn unit<'a>(&'a self) -> Option<&'a crate::UnitOfMeasure> {
        match self {
            TypeExpressionOrSubtype::AnonymousTypeExpression(val) => val.unit(),
            TypeExpressionOrSubtype::TypeDefinition(val) => val.unit(),
        }
    }
    fn implicit_prefix<'a>(&'a self) -> Option<&'a str> {
        match self {
            TypeExpressionOrSubtype::AnonymousTypeExpression(val) => val.implicit_prefix(),
            TypeExpressionOrSubtype::TypeDefinition(val) => val.implicit_prefix(),
        }
    }
    fn equals_string<'a>(&'a self) -> Option<&'a str> {
        match self {
            TypeExpressionOrSubtype::AnonymousTypeExpression(val) => val.equals_string(),
            TypeExpressionOrSubtype::TypeDefinition(val) => val.equals_string(),
        }
    }
    fn equals_string_in<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        match self {
            TypeExpressionOrSubtype::AnonymousTypeExpression(val) => {
                val.equals_string_in().map(|x| x.to_any())
            }
            TypeExpressionOrSubtype::TypeDefinition(val) => {
                val.equals_string_in().map(|x| x.to_any())
            }
        }
    }
    fn equals_number(&self) -> Option<isize> {
        match self {
            TypeExpressionOrSubtype::AnonymousTypeExpression(val) => val.equals_number(),
            TypeExpressionOrSubtype::TypeDefinition(val) => val.equals_number(),
        }
    }
    fn minimum_value<'a>(&'a self) -> Option<&'a crate::Anything> {
        match self {
            TypeExpressionOrSubtype::AnonymousTypeExpression(val) => val.minimum_value(),
            TypeExpressionOrSubtype::TypeDefinition(val) => val.minimum_value(),
        }
    }
    fn maximum_value<'a>(&'a self) -> Option<&'a crate::Anything> {
        match self {
            TypeExpressionOrSubtype::AnonymousTypeExpression(val) => val.maximum_value(),
            TypeExpressionOrSubtype::TypeDefinition(val) => val.maximum_value(),
        }
    }
    fn none_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousTypeExpression>> {
        match self {
            TypeExpressionOrSubtype::AnonymousTypeExpression(val) => {
                val.none_of().map(|x| x.to_any())
            }
            TypeExpressionOrSubtype::TypeDefinition(val) => val.none_of().map(|x| x.to_any()),
        }
    }
    fn exactly_one_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousTypeExpression>> {
        match self {
            TypeExpressionOrSubtype::AnonymousTypeExpression(val) => {
                val.exactly_one_of().map(|x| x.to_any())
            }
            TypeExpressionOrSubtype::TypeDefinition(val) => {
                val.exactly_one_of().map(|x| x.to_any())
            }
        }
    }
    fn any_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousTypeExpression>> {
        match self {
            TypeExpressionOrSubtype::AnonymousTypeExpression(val) => {
                val.any_of().map(|x| x.to_any())
            }
            TypeExpressionOrSubtype::TypeDefinition(val) => val.any_of().map(|x| x.to_any()),
        }
    }
    fn all_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousTypeExpression>> {
        match self {
            TypeExpressionOrSubtype::AnonymousTypeExpression(val) => {
                val.all_of().map(|x| x.to_any())
            }
            TypeExpressionOrSubtype::TypeDefinition(val) => val.all_of().map(|x| x.to_any()),
        }
    }
}

pub trait EnumExpression: Expression {
    fn code_set<'a>(&'a self) -> Option<&'a crate::uriorcurie>;
    // fn code_set_mut(&mut self) -> &mut Option<&'a crate::uriorcurie>;
    // fn set_code_set(&mut self, value: Option<&'a uriorcurie>);

    fn code_set_tag<'a>(&'a self) -> Option<&'a str>;
    // fn code_set_tag_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_code_set_tag(&mut self, value: Option<&'a str>);

    fn code_set_version<'a>(&'a self) -> Option<&'a str>;
    // fn code_set_version_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_code_set_version(&mut self, value: Option<&'a str>);

    fn pv_formula<'a>(&'a self) -> Option<&'a crate::PvFormulaOptions>;
    // fn pv_formula_mut(&mut self) -> &mut Option<&'a crate::PvFormulaOptions>;
    // fn set_pv_formula(&mut self, value: Option<&'a PvFormulaOptions>);

    fn permissible_values<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::PermissibleValue>>;
    // fn permissible_values_mut(&mut self) -> &mut Option<impl poly_containers::MapRef<'a, String,crate::PermissibleValue>>;
    // fn set_permissible_values<E>(&mut self, value: Option<&HashMap<String, E>>) where E: Into<PermissibleValue>;

    fn include<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousEnumExpression>>;
    // fn include_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, crate::AnonymousEnumExpression>>;
    // fn set_include<E>(&mut self, value: Option<&Vec<E>>) where E: Into<AnonymousEnumExpression>;

    fn minus<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousEnumExpression>>;
    // fn minus_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, crate::AnonymousEnumExpression>>;
    // fn set_minus<E>(&mut self, value: Option<&Vec<E>>) where E: Into<AnonymousEnumExpression>;

    fn inherits<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>>;
    // fn inherits_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, String>>;
    // fn set_inherits<E>(&mut self, value: Option<&Vec<String>>) where E: Into<String>;

    fn reachable_from<'a>(&'a self) -> Option<&'a crate::ReachabilityQuery>;
    // fn reachable_from_mut(&mut self) -> &mut Option<&'a crate::ReachabilityQuery>;
    // fn set_reachable_from<E>(&mut self, value: Option<E>) where E: Into<ReachabilityQuery>;

    fn matches<'a>(&'a self) -> Option<&'a crate::MatchQuery>;
    // fn matches_mut(&mut self) -> &mut Option<&'a crate::MatchQuery>;
    // fn set_matches<E>(&mut self, value: Option<E>) where E: Into<MatchQuery>;

    fn concepts<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>>;
    // fn concepts_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>>;
    // fn set_concepts(&mut self, value: Option<&Vec<uriorcurie>>);
}

impl EnumExpression for crate::EnumExpression {
    fn code_set<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.code_set.as_ref();
    }
    fn code_set_tag<'a>(&'a self) -> Option<&'a str> {
        return self.code_set_tag.as_deref();
    }
    fn code_set_version<'a>(&'a self) -> Option<&'a str> {
        return self.code_set_version.as_deref();
    }
    fn pv_formula<'a>(&'a self) -> Option<&'a crate::PvFormulaOptions> {
        return self.pv_formula.as_ref();
    }
    fn permissible_values<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::PermissibleValue>> {
        return self.permissible_values.as_ref();
    }
    fn include<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousEnumExpression>> {
        return self.include.as_ref();
    }
    fn minus<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousEnumExpression>> {
        return self.minus.as_ref();
    }
    fn inherits<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.inherits.as_ref();
    }
    fn reachable_from<'a>(&'a self) -> Option<&'a crate::ReachabilityQuery> {
        return self.reachable_from.as_ref();
    }
    fn matches<'a>(&'a self) -> Option<&'a crate::MatchQuery> {
        return self.matches.as_ref();
    }
    fn concepts<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.concepts.as_ref();
    }
}
impl EnumExpression for crate::AnonymousEnumExpression {
    fn code_set<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.code_set.as_ref();
    }
    fn code_set_tag<'a>(&'a self) -> Option<&'a str> {
        return self.code_set_tag.as_deref();
    }
    fn code_set_version<'a>(&'a self) -> Option<&'a str> {
        return self.code_set_version.as_deref();
    }
    fn pv_formula<'a>(&'a self) -> Option<&'a crate::PvFormulaOptions> {
        return self.pv_formula.as_ref();
    }
    fn permissible_values<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::PermissibleValue>> {
        return self.permissible_values.as_ref();
    }
    fn include<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousEnumExpression>> {
        return self
            .include
            .as_ref()
            .map(|x| poly_containers::ListView::new(x));
    }
    fn minus<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousEnumExpression>> {
        return self
            .minus
            .as_ref()
            .map(|x| poly_containers::ListView::new(x));
    }
    fn inherits<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.inherits.as_ref();
    }
    fn reachable_from<'a>(&'a self) -> Option<&'a crate::ReachabilityQuery> {
        return self.reachable_from.as_ref();
    }
    fn matches<'a>(&'a self) -> Option<&'a crate::MatchQuery> {
        return self.matches.as_ref();
    }
    fn concepts<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.concepts.as_ref();
    }
}
impl EnumExpression for crate::EnumDefinition {
    fn code_set<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.code_set.as_ref();
    }
    fn code_set_tag<'a>(&'a self) -> Option<&'a str> {
        return self.code_set_tag.as_deref();
    }
    fn code_set_version<'a>(&'a self) -> Option<&'a str> {
        return self.code_set_version.as_deref();
    }
    fn pv_formula<'a>(&'a self) -> Option<&'a crate::PvFormulaOptions> {
        return self.pv_formula.as_ref();
    }
    fn permissible_values<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::PermissibleValue>> {
        return self.permissible_values.as_ref();
    }
    fn include<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousEnumExpression>> {
        return self
            .include
            .as_ref()
            .map(|x| poly_containers::ListView::new(x));
    }
    fn minus<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousEnumExpression>> {
        return self
            .minus
            .as_ref()
            .map(|x| poly_containers::ListView::new(x));
    }
    fn inherits<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.inherits.as_ref();
    }
    fn reachable_from<'a>(&'a self) -> Option<&'a crate::ReachabilityQuery> {
        return self.reachable_from.as_ref();
    }
    fn matches<'a>(&'a self) -> Option<&'a crate::MatchQuery> {
        return self.matches.as_ref();
    }
    fn concepts<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.concepts.as_ref();
    }
}

impl EnumExpression for crate::EnumExpressionOrSubtype {
    fn code_set<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        match self {
            EnumExpressionOrSubtype::AnonymousEnumExpression(val) => val.code_set(),
            EnumExpressionOrSubtype::EnumDefinition(val) => val.code_set(),
        }
    }
    fn code_set_tag<'a>(&'a self) -> Option<&'a str> {
        match self {
            EnumExpressionOrSubtype::AnonymousEnumExpression(val) => val.code_set_tag(),
            EnumExpressionOrSubtype::EnumDefinition(val) => val.code_set_tag(),
        }
    }
    fn code_set_version<'a>(&'a self) -> Option<&'a str> {
        match self {
            EnumExpressionOrSubtype::AnonymousEnumExpression(val) => val.code_set_version(),
            EnumExpressionOrSubtype::EnumDefinition(val) => val.code_set_version(),
        }
    }
    fn pv_formula<'a>(&'a self) -> Option<&'a crate::PvFormulaOptions> {
        match self {
            EnumExpressionOrSubtype::AnonymousEnumExpression(val) => val.pv_formula(),
            EnumExpressionOrSubtype::EnumDefinition(val) => val.pv_formula(),
        }
    }
    fn permissible_values<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::PermissibleValue>> {
        match self {
            EnumExpressionOrSubtype::AnonymousEnumExpression(val) => {
                val.permissible_values().map(|x| x.to_any())
            }
            EnumExpressionOrSubtype::EnumDefinition(val) => {
                val.permissible_values().map(|x| x.to_any())
            }
        }
    }
    fn include<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousEnumExpression>> {
        match self {
            EnumExpressionOrSubtype::AnonymousEnumExpression(val) => {
                val.include().map(|x| x.to_any())
            }
            EnumExpressionOrSubtype::EnumDefinition(val) => val.include().map(|x| x.to_any()),
        }
    }
    fn minus<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousEnumExpression>> {
        match self {
            EnumExpressionOrSubtype::AnonymousEnumExpression(val) => {
                val.minus().map(|x| x.to_any())
            }
            EnumExpressionOrSubtype::EnumDefinition(val) => val.minus().map(|x| x.to_any()),
        }
    }
    fn inherits<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        match self {
            EnumExpressionOrSubtype::AnonymousEnumExpression(val) => {
                val.inherits().map(|x| x.to_any())
            }
            EnumExpressionOrSubtype::EnumDefinition(val) => val.inherits().map(|x| x.to_any()),
        }
    }
    fn reachable_from<'a>(&'a self) -> Option<&'a crate::ReachabilityQuery> {
        match self {
            EnumExpressionOrSubtype::AnonymousEnumExpression(val) => val.reachable_from(),
            EnumExpressionOrSubtype::EnumDefinition(val) => val.reachable_from(),
        }
    }
    fn matches<'a>(&'a self) -> Option<&'a crate::MatchQuery> {
        match self {
            EnumExpressionOrSubtype::AnonymousEnumExpression(val) => val.matches(),
            EnumExpressionOrSubtype::EnumDefinition(val) => val.matches(),
        }
    }
    fn concepts<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        match self {
            EnumExpressionOrSubtype::AnonymousEnumExpression(val) => {
                val.concepts().map(|x| x.to_any())
            }
            EnumExpressionOrSubtype::EnumDefinition(val) => val.concepts().map(|x| x.to_any()),
        }
    }
}

pub trait AnonymousExpression: Expression + Extensible + Annotatable + CommonMetadata {}

impl AnonymousExpression for crate::AnonymousExpression {}
impl AnonymousExpression for crate::AnonymousSlotExpression {}
impl AnonymousExpression for crate::AnonymousClassExpression {}

impl AnonymousExpression for crate::AnonymousExpressionOrSubtype {}

pub trait PathExpression: Expression + Extensible + Annotatable + CommonMetadata {
    fn followed_by<'a>(&'a self) -> Option<&'a crate::PathExpression>;
    // fn followed_by_mut(&mut self) -> &mut Option<&'a crate::PathExpression>;
    // fn set_followed_by<E>(&mut self, value: Option<E>) where E: Into<PathExpression>;

    fn none_of<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::PathExpression>>;
    // fn none_of_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, crate::PathExpression>>;
    // fn set_none_of<E>(&mut self, value: Option<&Vec<E>>) where E: Into<PathExpression>;

    fn any_of<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::PathExpression>>;
    // fn any_of_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, crate::PathExpression>>;
    // fn set_any_of<E>(&mut self, value: Option<&Vec<E>>) where E: Into<PathExpression>;

    fn all_of<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::PathExpression>>;
    // fn all_of_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, crate::PathExpression>>;
    // fn set_all_of<E>(&mut self, value: Option<&Vec<E>>) where E: Into<PathExpression>;

    fn exactly_one_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::PathExpression>>;
    // fn exactly_one_of_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, crate::PathExpression>>;
    // fn set_exactly_one_of<E>(&mut self, value: Option<&Vec<E>>) where E: Into<PathExpression>;

    fn reversed(&self) -> Option<bool>;
    // fn reversed_mut(&mut self) -> &mut Option<bool>;
    // fn set_reversed(&mut self, value: Option<bool>);

    fn traverse<'a>(&'a self) -> Option<&'a str>;
    // fn traverse_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_traverse<E>(&mut self, value: Option<&'a str>) where E: Into<String>;

    fn range_expression<'a>(&'a self) -> Option<&'a crate::AnonymousClassExpression>;
    // fn range_expression_mut(&mut self) -> &mut Option<&'a crate::AnonymousClassExpression>;
    // fn set_range_expression<E>(&mut self, value: Option<E>) where E: Into<AnonymousClassExpression>;
}

impl PathExpression for crate::PathExpression {
    fn followed_by<'a>(&'a self) -> Option<&'a crate::PathExpression> {
        return self.followed_by.as_deref();
    }
    fn none_of<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::PathExpression>> {
        return self
            .none_of
            .as_ref()
            .map(|x| poly_containers::ListView::new(x));
    }
    fn any_of<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::PathExpression>> {
        return self
            .any_of
            .as_ref()
            .map(|x| poly_containers::ListView::new(x));
    }
    fn all_of<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::PathExpression>> {
        return self
            .all_of
            .as_ref()
            .map(|x| poly_containers::ListView::new(x));
    }
    fn exactly_one_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::PathExpression>> {
        return self
            .exactly_one_of
            .as_ref()
            .map(|x| poly_containers::ListView::new(x));
    }
    fn reversed(&self) -> Option<bool> {
        return self.reversed;
    }
    fn traverse<'a>(&'a self) -> Option<&'a str> {
        return self.traverse.as_deref();
    }
    fn range_expression<'a>(&'a self) -> Option<&'a crate::AnonymousClassExpression> {
        return self.range_expression.as_deref();
    }
}

pub trait SlotExpression: Expression {
    fn range<'a>(&'a self) -> Option<&'a str>;
    // fn range_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_range<E>(&mut self, value: Option<&'a str>) where E: Into<String>;

    fn range_expression<'a>(&'a self) -> Option<&'a crate::AnonymousClassExpression>;
    // fn range_expression_mut(&mut self) -> &mut Option<&'a crate::AnonymousClassExpression>;
    // fn set_range_expression<E>(&mut self, value: Option<E>) where E: Into<AnonymousClassExpression>;

    fn enum_range<'a>(&'a self) -> Option<&'a EnumExpressionOrSubtype>;
    // fn enum_range_mut(&mut self) -> &mut Option<&'a EnumExpressionOrSubtype>;
    // fn set_enum_range<E>(&mut self, value: Option<E>) where E: Into<EnumExpression>;

    fn bindings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::EnumBinding>>;
    // fn bindings_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, crate::EnumBinding>>;
    // fn set_bindings<E>(&mut self, value: Option<&Vec<E>>) where E: Into<EnumBinding>;

    fn required(&self) -> Option<bool>;
    // fn required_mut(&mut self) -> &mut Option<bool>;
    // fn set_required(&mut self, value: Option<bool>);

    fn recommended(&self) -> Option<bool>;
    // fn recommended_mut(&mut self) -> &mut Option<bool>;
    // fn set_recommended(&mut self, value: Option<bool>);

    fn multivalued(&self) -> Option<bool>;
    // fn multivalued_mut(&mut self) -> &mut Option<bool>;
    // fn set_multivalued(&mut self, value: Option<bool>);

    fn inlined(&self) -> Option<bool>;
    // fn inlined_mut(&mut self) -> &mut Option<bool>;
    // fn set_inlined(&mut self, value: Option<bool>);

    fn inlined_as_list(&self) -> Option<bool>;
    // fn inlined_as_list_mut(&mut self) -> &mut Option<bool>;
    // fn set_inlined_as_list(&mut self, value: Option<bool>);

    fn minimum_value<'a>(&'a self) -> Option<&'a crate::Anything>;
    // fn minimum_value_mut(&mut self) -> &mut Option<&'a crate::Anything>;
    // fn set_minimum_value<E>(&mut self, value: Option<E>) where E: Into<Anything>;

    fn maximum_value<'a>(&'a self) -> Option<&'a crate::Anything>;
    // fn maximum_value_mut(&mut self) -> &mut Option<&'a crate::Anything>;
    // fn set_maximum_value<E>(&mut self, value: Option<E>) where E: Into<Anything>;

    fn pattern<'a>(&'a self) -> Option<&'a str>;
    // fn pattern_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_pattern(&mut self, value: Option<&'a str>);

    fn structured_pattern<'a>(&'a self) -> Option<&'a crate::PatternExpression>;
    // fn structured_pattern_mut(&mut self) -> &mut Option<&'a crate::PatternExpression>;
    // fn set_structured_pattern<E>(&mut self, value: Option<E>) where E: Into<PatternExpression>;

    fn unit<'a>(&'a self) -> Option<&'a crate::UnitOfMeasure>;
    // fn unit_mut(&mut self) -> &mut Option<&'a crate::UnitOfMeasure>;
    // fn set_unit<E>(&mut self, value: Option<E>) where E: Into<UnitOfMeasure>;

    fn implicit_prefix<'a>(&'a self) -> Option<&'a str>;
    // fn implicit_prefix_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_implicit_prefix(&mut self, value: Option<&'a str>);

    fn value_presence<'a>(&'a self) -> Option<&'a crate::PresenceEnum>;
    // fn value_presence_mut(&mut self) -> &mut Option<&'a crate::PresenceEnum>;
    // fn set_value_presence(&mut self, value: Option<&'a PresenceEnum>);

    fn equals_string<'a>(&'a self) -> Option<&'a str>;
    // fn equals_string_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_equals_string(&mut self, value: Option<&'a str>);

    fn equals_string_in<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>>;
    // fn equals_string_in_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, String>>;
    // fn set_equals_string_in(&mut self, value: Option<&Vec<String>>);

    fn equals_number(&self) -> Option<isize>;
    // fn equals_number_mut(&mut self) -> &mut Option<isize>;
    // fn set_equals_number(&mut self, value: Option<isize>);

    fn equals_expression<'a>(&'a self) -> Option<&'a str>;
    // fn equals_expression_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_equals_expression(&mut self, value: Option<&'a str>);

    fn exact_cardinality(&self) -> Option<isize>;
    // fn exact_cardinality_mut(&mut self) -> &mut Option<isize>;
    // fn set_exact_cardinality(&mut self, value: Option<isize>);

    fn minimum_cardinality(&self) -> Option<isize>;
    // fn minimum_cardinality_mut(&mut self) -> &mut Option<isize>;
    // fn set_minimum_cardinality(&mut self, value: Option<isize>);

    fn maximum_cardinality(&self) -> Option<isize>;
    // fn maximum_cardinality_mut(&mut self) -> &mut Option<isize>;
    // fn set_maximum_cardinality(&mut self, value: Option<isize>);

    fn has_member<'a>(&'a self) -> Option<&'a crate::AnonymousSlotExpression>;
    // fn has_member_mut(&mut self) -> &mut Option<&'a crate::AnonymousSlotExpression>;
    // fn set_has_member<E>(&mut self, value: Option<E>) where E: Into<AnonymousSlotExpression>;

    fn all_members<'a>(&'a self) -> Option<&'a crate::AnonymousSlotExpression>;
    // fn all_members_mut(&mut self) -> &mut Option<&'a crate::AnonymousSlotExpression>;
    // fn set_all_members<E>(&mut self, value: Option<E>) where E: Into<AnonymousSlotExpression>;

    fn none_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousSlotExpression>>;
    // fn none_of_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, crate::AnonymousSlotExpression>>;
    // fn set_none_of<E>(&mut self, value: Option<&Vec<E>>) where E: Into<AnonymousSlotExpression>;

    fn exactly_one_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousSlotExpression>>;
    // fn exactly_one_of_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, crate::AnonymousSlotExpression>>;
    // fn set_exactly_one_of<E>(&mut self, value: Option<&Vec<E>>) where E: Into<AnonymousSlotExpression>;

    fn any_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousSlotExpression>>;
    // fn any_of_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, crate::AnonymousSlotExpression>>;
    // fn set_any_of<E>(&mut self, value: Option<&Vec<E>>) where E: Into<AnonymousSlotExpression>;

    fn all_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousSlotExpression>>;
    // fn all_of_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, crate::AnonymousSlotExpression>>;
    // fn set_all_of<E>(&mut self, value: Option<&Vec<E>>) where E: Into<AnonymousSlotExpression>;

    fn array<'a>(&'a self) -> Option<&'a crate::ArrayExpression>;
    // fn array_mut(&mut self) -> &mut Option<&'a crate::ArrayExpression>;
    // fn set_array<E>(&mut self, value: Option<E>) where E: Into<ArrayExpression>;
}

impl SlotExpression for crate::SlotExpression {
    fn range<'a>(&'a self) -> Option<&'a str> {
        return self.range.as_deref();
    }
    fn range_expression<'a>(&'a self) -> Option<&'a crate::AnonymousClassExpression> {
        return self.range_expression.as_ref();
    }
    fn enum_range<'a>(&'a self) -> Option<&'a EnumExpressionOrSubtype> {
        return self.enum_range.as_ref();
    }
    fn bindings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::EnumBinding>> {
        return self.bindings.as_ref();
    }
    fn required(&self) -> Option<bool> {
        return self.required;
    }
    fn recommended(&self) -> Option<bool> {
        return self.recommended;
    }
    fn multivalued(&self) -> Option<bool> {
        return self.multivalued;
    }
    fn inlined(&self) -> Option<bool> {
        return self.inlined;
    }
    fn inlined_as_list(&self) -> Option<bool> {
        return self.inlined_as_list;
    }
    fn minimum_value<'a>(&'a self) -> Option<&'a crate::Anything> {
        return self.minimum_value.as_ref();
    }
    fn maximum_value<'a>(&'a self) -> Option<&'a crate::Anything> {
        return self.maximum_value.as_ref();
    }
    fn pattern<'a>(&'a self) -> Option<&'a str> {
        return self.pattern.as_deref();
    }
    fn structured_pattern<'a>(&'a self) -> Option<&'a crate::PatternExpression> {
        return self.structured_pattern.as_ref();
    }
    fn unit<'a>(&'a self) -> Option<&'a crate::UnitOfMeasure> {
        return self.unit.as_ref();
    }
    fn implicit_prefix<'a>(&'a self) -> Option<&'a str> {
        return self.implicit_prefix.as_deref();
    }
    fn value_presence<'a>(&'a self) -> Option<&'a crate::PresenceEnum> {
        return self.value_presence.as_ref();
    }
    fn equals_string<'a>(&'a self) -> Option<&'a str> {
        return self.equals_string.as_deref();
    }
    fn equals_string_in<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.equals_string_in.as_ref();
    }
    fn equals_number(&self) -> Option<isize> {
        return self.equals_number;
    }
    fn equals_expression<'a>(&'a self) -> Option<&'a str> {
        return self.equals_expression.as_deref();
    }
    fn exact_cardinality(&self) -> Option<isize> {
        return self.exact_cardinality;
    }
    fn minimum_cardinality(&self) -> Option<isize> {
        return self.minimum_cardinality;
    }
    fn maximum_cardinality(&self) -> Option<isize> {
        return self.maximum_cardinality;
    }
    fn has_member<'a>(&'a self) -> Option<&'a crate::AnonymousSlotExpression> {
        return self.has_member.as_ref();
    }
    fn all_members<'a>(&'a self) -> Option<&'a crate::AnonymousSlotExpression> {
        return self.all_members.as_ref();
    }
    fn none_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousSlotExpression>> {
        return self.none_of.as_ref();
    }
    fn exactly_one_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousSlotExpression>> {
        return self.exactly_one_of.as_ref();
    }
    fn any_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousSlotExpression>> {
        return self.any_of.as_ref();
    }
    fn all_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousSlotExpression>> {
        return self.all_of.as_ref();
    }
    fn array<'a>(&'a self) -> Option<&'a crate::ArrayExpression> {
        return self.array.as_ref();
    }
}
impl SlotExpression for crate::AnonymousSlotExpression {
    fn range<'a>(&'a self) -> Option<&'a str> {
        return self.range.as_deref();
    }
    fn range_expression<'a>(&'a self) -> Option<&'a crate::AnonymousClassExpression> {
        return self.range_expression.as_deref();
    }
    fn enum_range<'a>(&'a self) -> Option<&'a EnumExpressionOrSubtype> {
        return self.enum_range.as_ref();
    }
    fn bindings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::EnumBinding>> {
        return self.bindings.as_ref();
    }
    fn required(&self) -> Option<bool> {
        return self.required;
    }
    fn recommended(&self) -> Option<bool> {
        return self.recommended;
    }
    fn multivalued(&self) -> Option<bool> {
        return self.multivalued;
    }
    fn inlined(&self) -> Option<bool> {
        return self.inlined;
    }
    fn inlined_as_list(&self) -> Option<bool> {
        return self.inlined_as_list;
    }
    fn minimum_value<'a>(&'a self) -> Option<&'a crate::Anything> {
        return self.minimum_value.as_ref();
    }
    fn maximum_value<'a>(&'a self) -> Option<&'a crate::Anything> {
        return self.maximum_value.as_ref();
    }
    fn pattern<'a>(&'a self) -> Option<&'a str> {
        return self.pattern.as_deref();
    }
    fn structured_pattern<'a>(&'a self) -> Option<&'a crate::PatternExpression> {
        return self.structured_pattern.as_ref();
    }
    fn unit<'a>(&'a self) -> Option<&'a crate::UnitOfMeasure> {
        return self.unit.as_ref();
    }
    fn implicit_prefix<'a>(&'a self) -> Option<&'a str> {
        return self.implicit_prefix.as_deref();
    }
    fn value_presence<'a>(&'a self) -> Option<&'a crate::PresenceEnum> {
        return self.value_presence.as_ref();
    }
    fn equals_string<'a>(&'a self) -> Option<&'a str> {
        return self.equals_string.as_deref();
    }
    fn equals_string_in<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.equals_string_in.as_ref();
    }
    fn equals_number(&self) -> Option<isize> {
        return self.equals_number;
    }
    fn equals_expression<'a>(&'a self) -> Option<&'a str> {
        return self.equals_expression.as_deref();
    }
    fn exact_cardinality(&self) -> Option<isize> {
        return self.exact_cardinality;
    }
    fn minimum_cardinality(&self) -> Option<isize> {
        return self.minimum_cardinality;
    }
    fn maximum_cardinality(&self) -> Option<isize> {
        return self.maximum_cardinality;
    }
    fn has_member<'a>(&'a self) -> Option<&'a crate::AnonymousSlotExpression> {
        return self.has_member.as_deref();
    }
    fn all_members<'a>(&'a self) -> Option<&'a crate::AnonymousSlotExpression> {
        return self.all_members.as_deref();
    }
    fn none_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousSlotExpression>> {
        return self
            .none_of
            .as_ref()
            .map(|x| poly_containers::ListView::new(x));
    }
    fn exactly_one_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousSlotExpression>> {
        return self
            .exactly_one_of
            .as_ref()
            .map(|x| poly_containers::ListView::new(x));
    }
    fn any_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousSlotExpression>> {
        return self
            .any_of
            .as_ref()
            .map(|x| poly_containers::ListView::new(x));
    }
    fn all_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousSlotExpression>> {
        return self
            .all_of
            .as_ref()
            .map(|x| poly_containers::ListView::new(x));
    }
    fn array<'a>(&'a self) -> Option<&'a crate::ArrayExpression> {
        return self.array.as_ref();
    }
}
impl SlotExpression for crate::SlotDefinition {
    fn range<'a>(&'a self) -> Option<&'a str> {
        return self.range.as_deref();
    }
    fn range_expression<'a>(&'a self) -> Option<&'a crate::AnonymousClassExpression> {
        return self.range_expression.as_deref();
    }
    fn enum_range<'a>(&'a self) -> Option<&'a EnumExpressionOrSubtype> {
        return self.enum_range.as_ref();
    }
    fn bindings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::EnumBinding>> {
        return self.bindings.as_ref();
    }
    fn required(&self) -> Option<bool> {
        return self.required;
    }
    fn recommended(&self) -> Option<bool> {
        return self.recommended;
    }
    fn multivalued(&self) -> Option<bool> {
        return self.multivalued;
    }
    fn inlined(&self) -> Option<bool> {
        return self.inlined;
    }
    fn inlined_as_list(&self) -> Option<bool> {
        return self.inlined_as_list;
    }
    fn minimum_value<'a>(&'a self) -> Option<&'a crate::Anything> {
        return self.minimum_value.as_ref();
    }
    fn maximum_value<'a>(&'a self) -> Option<&'a crate::Anything> {
        return self.maximum_value.as_ref();
    }
    fn pattern<'a>(&'a self) -> Option<&'a str> {
        return self.pattern.as_deref();
    }
    fn structured_pattern<'a>(&'a self) -> Option<&'a crate::PatternExpression> {
        return self.structured_pattern.as_ref();
    }
    fn unit<'a>(&'a self) -> Option<&'a crate::UnitOfMeasure> {
        return self.unit.as_ref();
    }
    fn implicit_prefix<'a>(&'a self) -> Option<&'a str> {
        return self.implicit_prefix.as_deref();
    }
    fn value_presence<'a>(&'a self) -> Option<&'a crate::PresenceEnum> {
        return self.value_presence.as_ref();
    }
    fn equals_string<'a>(&'a self) -> Option<&'a str> {
        return self.equals_string.as_deref();
    }
    fn equals_string_in<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.equals_string_in.as_ref();
    }
    fn equals_number(&self) -> Option<isize> {
        return self.equals_number;
    }
    fn equals_expression<'a>(&'a self) -> Option<&'a str> {
        return self.equals_expression.as_deref();
    }
    fn exact_cardinality(&self) -> Option<isize> {
        return self.exact_cardinality;
    }
    fn minimum_cardinality(&self) -> Option<isize> {
        return self.minimum_cardinality;
    }
    fn maximum_cardinality(&self) -> Option<isize> {
        return self.maximum_cardinality;
    }
    fn has_member<'a>(&'a self) -> Option<&'a crate::AnonymousSlotExpression> {
        return self.has_member.as_deref();
    }
    fn all_members<'a>(&'a self) -> Option<&'a crate::AnonymousSlotExpression> {
        return self.all_members.as_deref();
    }
    fn none_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousSlotExpression>> {
        return self
            .none_of
            .as_ref()
            .map(|x| poly_containers::ListView::new(x));
    }
    fn exactly_one_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousSlotExpression>> {
        return self
            .exactly_one_of
            .as_ref()
            .map(|x| poly_containers::ListView::new(x));
    }
    fn any_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousSlotExpression>> {
        return self
            .any_of
            .as_ref()
            .map(|x| poly_containers::ListView::new(x));
    }
    fn all_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousSlotExpression>> {
        return self
            .all_of
            .as_ref()
            .map(|x| poly_containers::ListView::new(x));
    }
    fn array<'a>(&'a self) -> Option<&'a crate::ArrayExpression> {
        return self.array.as_ref();
    }
}

impl SlotExpression for crate::SlotExpressionOrSubtype {
    fn range<'a>(&'a self) -> Option<&'a str> {
        match self {
            SlotExpressionOrSubtype::AnonymousSlotExpression(val) => val.range(),
            SlotExpressionOrSubtype::SlotDefinition(val) => val.range(),
        }
    }
    fn range_expression<'a>(&'a self) -> Option<&'a crate::AnonymousClassExpression> {
        match self {
            SlotExpressionOrSubtype::AnonymousSlotExpression(val) => val.range_expression(),
            SlotExpressionOrSubtype::SlotDefinition(val) => val.range_expression(),
        }
    }
    fn enum_range<'a>(&'a self) -> Option<&'a EnumExpressionOrSubtype> {
        match self {
            SlotExpressionOrSubtype::AnonymousSlotExpression(val) => val.enum_range(),
            SlotExpressionOrSubtype::SlotDefinition(val) => val.enum_range(),
        }
    }
    fn bindings<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::EnumBinding>> {
        match self {
            SlotExpressionOrSubtype::AnonymousSlotExpression(val) => {
                val.bindings().map(|x| x.to_any())
            }
            SlotExpressionOrSubtype::SlotDefinition(val) => val.bindings().map(|x| x.to_any()),
        }
    }
    fn required(&self) -> Option<bool> {
        match self {
            SlotExpressionOrSubtype::AnonymousSlotExpression(val) => val.required(),
            SlotExpressionOrSubtype::SlotDefinition(val) => val.required(),
        }
    }
    fn recommended(&self) -> Option<bool> {
        match self {
            SlotExpressionOrSubtype::AnonymousSlotExpression(val) => val.recommended(),
            SlotExpressionOrSubtype::SlotDefinition(val) => val.recommended(),
        }
    }
    fn multivalued(&self) -> Option<bool> {
        match self {
            SlotExpressionOrSubtype::AnonymousSlotExpression(val) => val.multivalued(),
            SlotExpressionOrSubtype::SlotDefinition(val) => val.multivalued(),
        }
    }
    fn inlined(&self) -> Option<bool> {
        match self {
            SlotExpressionOrSubtype::AnonymousSlotExpression(val) => val.inlined(),
            SlotExpressionOrSubtype::SlotDefinition(val) => val.inlined(),
        }
    }
    fn inlined_as_list(&self) -> Option<bool> {
        match self {
            SlotExpressionOrSubtype::AnonymousSlotExpression(val) => val.inlined_as_list(),
            SlotExpressionOrSubtype::SlotDefinition(val) => val.inlined_as_list(),
        }
    }
    fn minimum_value<'a>(&'a self) -> Option<&'a crate::Anything> {
        match self {
            SlotExpressionOrSubtype::AnonymousSlotExpression(val) => val.minimum_value(),
            SlotExpressionOrSubtype::SlotDefinition(val) => val.minimum_value(),
        }
    }
    fn maximum_value<'a>(&'a self) -> Option<&'a crate::Anything> {
        match self {
            SlotExpressionOrSubtype::AnonymousSlotExpression(val) => val.maximum_value(),
            SlotExpressionOrSubtype::SlotDefinition(val) => val.maximum_value(),
        }
    }
    fn pattern<'a>(&'a self) -> Option<&'a str> {
        match self {
            SlotExpressionOrSubtype::AnonymousSlotExpression(val) => val.pattern(),
            SlotExpressionOrSubtype::SlotDefinition(val) => val.pattern(),
        }
    }
    fn structured_pattern<'a>(&'a self) -> Option<&'a crate::PatternExpression> {
        match self {
            SlotExpressionOrSubtype::AnonymousSlotExpression(val) => val.structured_pattern(),
            SlotExpressionOrSubtype::SlotDefinition(val) => val.structured_pattern(),
        }
    }
    fn unit<'a>(&'a self) -> Option<&'a crate::UnitOfMeasure> {
        match self {
            SlotExpressionOrSubtype::AnonymousSlotExpression(val) => val.unit(),
            SlotExpressionOrSubtype::SlotDefinition(val) => val.unit(),
        }
    }
    fn implicit_prefix<'a>(&'a self) -> Option<&'a str> {
        match self {
            SlotExpressionOrSubtype::AnonymousSlotExpression(val) => val.implicit_prefix(),
            SlotExpressionOrSubtype::SlotDefinition(val) => val.implicit_prefix(),
        }
    }
    fn value_presence<'a>(&'a self) -> Option<&'a crate::PresenceEnum> {
        match self {
            SlotExpressionOrSubtype::AnonymousSlotExpression(val) => val.value_presence(),
            SlotExpressionOrSubtype::SlotDefinition(val) => val.value_presence(),
        }
    }
    fn equals_string<'a>(&'a self) -> Option<&'a str> {
        match self {
            SlotExpressionOrSubtype::AnonymousSlotExpression(val) => val.equals_string(),
            SlotExpressionOrSubtype::SlotDefinition(val) => val.equals_string(),
        }
    }
    fn equals_string_in<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        match self {
            SlotExpressionOrSubtype::AnonymousSlotExpression(val) => {
                val.equals_string_in().map(|x| x.to_any())
            }
            SlotExpressionOrSubtype::SlotDefinition(val) => {
                val.equals_string_in().map(|x| x.to_any())
            }
        }
    }
    fn equals_number(&self) -> Option<isize> {
        match self {
            SlotExpressionOrSubtype::AnonymousSlotExpression(val) => val.equals_number(),
            SlotExpressionOrSubtype::SlotDefinition(val) => val.equals_number(),
        }
    }
    fn equals_expression<'a>(&'a self) -> Option<&'a str> {
        match self {
            SlotExpressionOrSubtype::AnonymousSlotExpression(val) => val.equals_expression(),
            SlotExpressionOrSubtype::SlotDefinition(val) => val.equals_expression(),
        }
    }
    fn exact_cardinality(&self) -> Option<isize> {
        match self {
            SlotExpressionOrSubtype::AnonymousSlotExpression(val) => val.exact_cardinality(),
            SlotExpressionOrSubtype::SlotDefinition(val) => val.exact_cardinality(),
        }
    }
    fn minimum_cardinality(&self) -> Option<isize> {
        match self {
            SlotExpressionOrSubtype::AnonymousSlotExpression(val) => val.minimum_cardinality(),
            SlotExpressionOrSubtype::SlotDefinition(val) => val.minimum_cardinality(),
        }
    }
    fn maximum_cardinality(&self) -> Option<isize> {
        match self {
            SlotExpressionOrSubtype::AnonymousSlotExpression(val) => val.maximum_cardinality(),
            SlotExpressionOrSubtype::SlotDefinition(val) => val.maximum_cardinality(),
        }
    }
    fn has_member<'a>(&'a self) -> Option<&'a crate::AnonymousSlotExpression> {
        match self {
            SlotExpressionOrSubtype::AnonymousSlotExpression(val) => val.has_member(),
            SlotExpressionOrSubtype::SlotDefinition(val) => val.has_member(),
        }
    }
    fn all_members<'a>(&'a self) -> Option<&'a crate::AnonymousSlotExpression> {
        match self {
            SlotExpressionOrSubtype::AnonymousSlotExpression(val) => val.all_members(),
            SlotExpressionOrSubtype::SlotDefinition(val) => val.all_members(),
        }
    }
    fn none_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousSlotExpression>> {
        match self {
            SlotExpressionOrSubtype::AnonymousSlotExpression(val) => {
                val.none_of().map(|x| x.to_any())
            }
            SlotExpressionOrSubtype::SlotDefinition(val) => val.none_of().map(|x| x.to_any()),
        }
    }
    fn exactly_one_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousSlotExpression>> {
        match self {
            SlotExpressionOrSubtype::AnonymousSlotExpression(val) => {
                val.exactly_one_of().map(|x| x.to_any())
            }
            SlotExpressionOrSubtype::SlotDefinition(val) => {
                val.exactly_one_of().map(|x| x.to_any())
            }
        }
    }
    fn any_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousSlotExpression>> {
        match self {
            SlotExpressionOrSubtype::AnonymousSlotExpression(val) => {
                val.any_of().map(|x| x.to_any())
            }
            SlotExpressionOrSubtype::SlotDefinition(val) => val.any_of().map(|x| x.to_any()),
        }
    }
    fn all_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousSlotExpression>> {
        match self {
            SlotExpressionOrSubtype::AnonymousSlotExpression(val) => {
                val.all_of().map(|x| x.to_any())
            }
            SlotExpressionOrSubtype::SlotDefinition(val) => val.all_of().map(|x| x.to_any()),
        }
    }
    fn array<'a>(&'a self) -> Option<&'a crate::ArrayExpression> {
        match self {
            SlotExpressionOrSubtype::AnonymousSlotExpression(val) => val.array(),
            SlotExpressionOrSubtype::SlotDefinition(val) => val.array(),
        }
    }
}

pub trait AnonymousSlotExpression: AnonymousExpression + SlotExpression {}

impl AnonymousSlotExpression for crate::AnonymousSlotExpression {}

pub trait SlotDefinition: Definition + SlotExpression {
    fn singular_name<'a>(&'a self) -> Option<&'a str>;
    // fn singular_name_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_singular_name(&mut self, value: Option<&'a str>);

    fn domain<'a>(&'a self) -> Option<&'a str>;
    // fn domain_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_domain<E>(&mut self, value: Option<&'a str>) where E: Into<String>;

    fn slot_uri<'a>(&'a self) -> Option<&'a crate::uriorcurie>;
    // fn slot_uri_mut(&mut self) -> &mut Option<&'a crate::uriorcurie>;
    // fn set_slot_uri(&mut self, value: Option<&'a uriorcurie>);

    fn inherited(&self) -> Option<bool>;
    // fn inherited_mut(&mut self) -> &mut Option<bool>;
    // fn set_inherited(&mut self, value: Option<bool>);

    fn readonly<'a>(&'a self) -> Option<&'a str>;
    // fn readonly_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_readonly(&mut self, value: Option<&'a str>);

    fn ifabsent<'a>(&'a self) -> Option<&'a str>;
    // fn ifabsent_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_ifabsent(&mut self, value: Option<&'a str>);

    fn list_elements_unique(&self) -> Option<bool>;
    // fn list_elements_unique_mut(&mut self) -> &mut Option<bool>;
    // fn set_list_elements_unique(&mut self, value: Option<bool>);

    fn list_elements_ordered(&self) -> Option<bool>;
    // fn list_elements_ordered_mut(&mut self) -> &mut Option<bool>;
    // fn set_list_elements_ordered(&mut self, value: Option<bool>);

    fn shared(&self) -> Option<bool>;
    // fn shared_mut(&mut self) -> &mut Option<bool>;
    // fn set_shared(&mut self, value: Option<bool>);

    fn key(&self) -> Option<bool>;
    // fn key_mut(&mut self) -> &mut Option<bool>;
    // fn set_key(&mut self, value: Option<bool>);

    fn identifier(&self) -> Option<bool>;
    // fn identifier_mut(&mut self) -> &mut Option<bool>;
    // fn set_identifier(&mut self, value: Option<bool>);

    fn designates_type(&self) -> Option<bool>;
    // fn designates_type_mut(&mut self) -> &mut Option<bool>;
    // fn set_designates_type(&mut self, value: Option<bool>);

    fn alias<'a>(&'a self) -> Option<&'a str>;
    // fn alias_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_alias(&mut self, value: Option<&'a str>);

    fn owner<'a>(&'a self) -> Option<&'a str>;
    // fn owner_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_owner<E>(&mut self, value: Option<&'a str>) where E: Into<String>;

    fn domain_of<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>>;
    // fn domain_of_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, String>>;
    // fn set_domain_of<E>(&mut self, value: Option<&Vec<String>>) where E: Into<String>;

    fn subproperty_of<'a>(&'a self) -> Option<&'a str>;
    // fn subproperty_of_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_subproperty_of<E>(&mut self, value: Option<&'a str>) where E: Into<String>;

    fn symmetric(&self) -> Option<bool>;
    // fn symmetric_mut(&mut self) -> &mut Option<bool>;
    // fn set_symmetric(&mut self, value: Option<bool>);

    fn reflexive(&self) -> Option<bool>;
    // fn reflexive_mut(&mut self) -> &mut Option<bool>;
    // fn set_reflexive(&mut self, value: Option<bool>);

    fn locally_reflexive(&self) -> Option<bool>;
    // fn locally_reflexive_mut(&mut self) -> &mut Option<bool>;
    // fn set_locally_reflexive(&mut self, value: Option<bool>);

    fn irreflexive(&self) -> Option<bool>;
    // fn irreflexive_mut(&mut self) -> &mut Option<bool>;
    // fn set_irreflexive(&mut self, value: Option<bool>);

    fn asymmetric(&self) -> Option<bool>;
    // fn asymmetric_mut(&mut self) -> &mut Option<bool>;
    // fn set_asymmetric(&mut self, value: Option<bool>);

    fn transitive(&self) -> Option<bool>;
    // fn transitive_mut(&mut self) -> &mut Option<bool>;
    // fn set_transitive(&mut self, value: Option<bool>);

    fn inverse<'a>(&'a self) -> Option<&'a str>;
    // fn inverse_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_inverse<E>(&mut self, value: Option<&'a str>) where E: Into<String>;

    fn is_class_field(&self) -> Option<bool>;
    // fn is_class_field_mut(&mut self) -> &mut Option<bool>;
    // fn set_is_class_field(&mut self, value: Option<bool>);

    fn transitive_form_of<'a>(&'a self) -> Option<&'a str>;
    // fn transitive_form_of_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_transitive_form_of<E>(&mut self, value: Option<&'a str>) where E: Into<String>;

    fn reflexive_transitive_form_of<'a>(&'a self) -> Option<&'a str>;
    // fn reflexive_transitive_form_of_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_reflexive_transitive_form_of<E>(&mut self, value: Option<&'a str>) where E: Into<String>;

    fn role<'a>(&'a self) -> Option<&'a str>;
    // fn role_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_role(&mut self, value: Option<&'a str>);

    fn is_usage_slot(&self) -> Option<bool>;
    // fn is_usage_slot_mut(&mut self) -> &mut Option<bool>;
    // fn set_is_usage_slot(&mut self, value: Option<bool>);

    fn usage_slot_name<'a>(&'a self) -> Option<&'a str>;
    // fn usage_slot_name_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_usage_slot_name(&mut self, value: Option<&'a str>);

    fn relational_role<'a>(&'a self) -> Option<&'a crate::RelationalRoleEnum>;
    // fn relational_role_mut(&mut self) -> &mut Option<&'a crate::RelationalRoleEnum>;
    // fn set_relational_role(&mut self, value: Option<&'a RelationalRoleEnum>);

    fn slot_group<'a>(&'a self) -> Option<&'a str>;
    // fn slot_group_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_slot_group<E>(&mut self, value: Option<&'a str>) where E: Into<String>;

    fn is_grouping_slot(&self) -> Option<bool>;
    // fn is_grouping_slot_mut(&mut self) -> &mut Option<bool>;
    // fn set_is_grouping_slot(&mut self, value: Option<bool>);

    fn path_rule<'a>(&'a self) -> Option<&'a crate::PathExpression>;
    // fn path_rule_mut(&mut self) -> &mut Option<&'a crate::PathExpression>;
    // fn set_path_rule<E>(&mut self, value: Option<E>) where E: Into<PathExpression>;

    fn disjoint_with<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>>;
    // fn disjoint_with_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, String>>;
    // fn set_disjoint_with<E>(&mut self, value: Option<&Vec<String>>) where E: Into<String>;

    fn children_are_mutually_disjoint(&self) -> Option<bool>;
    // fn children_are_mutually_disjoint_mut(&mut self) -> &mut Option<bool>;
    // fn set_children_are_mutually_disjoint(&mut self, value: Option<bool>);

    fn union_of<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>>;
    // fn union_of_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, String>>;
    // fn set_union_of<E>(&mut self, value: Option<&Vec<String>>) where E: Into<String>;

    fn type_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::TypeMapping>>;
    // fn type_mappings_mut(&mut self) -> &mut Option<impl poly_containers::MapRef<'a, String,crate::TypeMapping>>;
    // fn set_type_mappings<E>(&mut self, value: Option<&HashMap<String, E>>) where E: Into<TypeMapping>;
}

impl SlotDefinition for crate::SlotDefinition {
    fn singular_name<'a>(&'a self) -> Option<&'a str> {
        return self.singular_name.as_deref();
    }
    fn domain<'a>(&'a self) -> Option<&'a str> {
        return self.domain.as_deref();
    }
    fn slot_uri<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.slot_uri.as_ref();
    }
    fn inherited(&self) -> Option<bool> {
        return self.inherited;
    }
    fn readonly<'a>(&'a self) -> Option<&'a str> {
        return self.readonly.as_deref();
    }
    fn ifabsent<'a>(&'a self) -> Option<&'a str> {
        return self.ifabsent.as_deref();
    }
    fn list_elements_unique(&self) -> Option<bool> {
        return self.list_elements_unique;
    }
    fn list_elements_ordered(&self) -> Option<bool> {
        return self.list_elements_ordered;
    }
    fn shared(&self) -> Option<bool> {
        return self.shared;
    }
    fn key(&self) -> Option<bool> {
        return self.key;
    }
    fn identifier(&self) -> Option<bool> {
        return self.identifier;
    }
    fn designates_type(&self) -> Option<bool> {
        return self.designates_type;
    }
    fn alias<'a>(&'a self) -> Option<&'a str> {
        return self.alias.as_deref();
    }
    fn owner<'a>(&'a self) -> Option<&'a str> {
        return self.owner.as_deref();
    }
    fn domain_of<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.domain_of.as_ref();
    }
    fn subproperty_of<'a>(&'a self) -> Option<&'a str> {
        return self.subproperty_of.as_deref();
    }
    fn symmetric(&self) -> Option<bool> {
        return self.symmetric;
    }
    fn reflexive(&self) -> Option<bool> {
        return self.reflexive;
    }
    fn locally_reflexive(&self) -> Option<bool> {
        return self.locally_reflexive;
    }
    fn irreflexive(&self) -> Option<bool> {
        return self.irreflexive;
    }
    fn asymmetric(&self) -> Option<bool> {
        return self.asymmetric;
    }
    fn transitive(&self) -> Option<bool> {
        return self.transitive;
    }
    fn inverse<'a>(&'a self) -> Option<&'a str> {
        return self.inverse.as_deref();
    }
    fn is_class_field(&self) -> Option<bool> {
        return self.is_class_field;
    }
    fn transitive_form_of<'a>(&'a self) -> Option<&'a str> {
        return self.transitive_form_of.as_deref();
    }
    fn reflexive_transitive_form_of<'a>(&'a self) -> Option<&'a str> {
        return self.reflexive_transitive_form_of.as_deref();
    }
    fn role<'a>(&'a self) -> Option<&'a str> {
        return self.role.as_deref();
    }
    fn is_usage_slot(&self) -> Option<bool> {
        return self.is_usage_slot;
    }
    fn usage_slot_name<'a>(&'a self) -> Option<&'a str> {
        return self.usage_slot_name.as_deref();
    }
    fn relational_role<'a>(&'a self) -> Option<&'a crate::RelationalRoleEnum> {
        return self.relational_role.as_ref();
    }
    fn slot_group<'a>(&'a self) -> Option<&'a str> {
        return self.slot_group.as_deref();
    }
    fn is_grouping_slot(&self) -> Option<bool> {
        return self.is_grouping_slot;
    }
    fn path_rule<'a>(&'a self) -> Option<&'a crate::PathExpression> {
        return self.path_rule.as_deref();
    }
    fn disjoint_with<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.disjoint_with.as_ref();
    }
    fn children_are_mutually_disjoint(&self) -> Option<bool> {
        return self.children_are_mutually_disjoint;
    }
    fn union_of<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.union_of.as_ref();
    }
    fn type_mappings<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::TypeMapping>> {
        return self.type_mappings.as_ref();
    }
}

pub trait ClassExpression {
    fn any_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousClassExpression>>;
    // fn any_of_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, crate::AnonymousClassExpression>>;
    // fn set_any_of<E>(&mut self, value: Option<&Vec<E>>) where E: Into<AnonymousClassExpression>;

    fn exactly_one_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousClassExpression>>;
    // fn exactly_one_of_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, crate::AnonymousClassExpression>>;
    // fn set_exactly_one_of<E>(&mut self, value: Option<&Vec<E>>) where E: Into<AnonymousClassExpression>;

    fn none_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousClassExpression>>;
    // fn none_of_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, crate::AnonymousClassExpression>>;
    // fn set_none_of<E>(&mut self, value: Option<&Vec<E>>) where E: Into<AnonymousClassExpression>;

    fn all_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousClassExpression>>;
    // fn all_of_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, crate::AnonymousClassExpression>>;
    // fn set_all_of<E>(&mut self, value: Option<&Vec<E>>) where E: Into<AnonymousClassExpression>;

    fn slot_conditions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::SlotDefinition>>;
    // fn slot_conditions_mut(&mut self) -> &mut Option<impl poly_containers::MapRef<'a, String,crate::SlotDefinition>>;
    // fn set_slot_conditions<E>(&mut self, value: Option<&HashMap<String, E>>) where E: Into<SlotDefinition>;
}

impl ClassExpression for crate::ClassExpression {
    fn any_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousClassExpression>> {
        return self.any_of.as_ref();
    }
    fn exactly_one_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousClassExpression>> {
        return self.exactly_one_of.as_ref();
    }
    fn none_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousClassExpression>> {
        return self.none_of.as_ref();
    }
    fn all_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousClassExpression>> {
        return self.all_of.as_ref();
    }
    fn slot_conditions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::SlotDefinition>> {
        return self.slot_conditions.as_ref();
    }
}
impl ClassExpression for crate::AnonymousClassExpression {
    fn any_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousClassExpression>> {
        return self
            .any_of
            .as_ref()
            .map(|x| poly_containers::ListView::new(x));
    }
    fn exactly_one_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousClassExpression>> {
        return self
            .exactly_one_of
            .as_ref()
            .map(|x| poly_containers::ListView::new(x));
    }
    fn none_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousClassExpression>> {
        return self
            .none_of
            .as_ref()
            .map(|x| poly_containers::ListView::new(x));
    }
    fn all_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousClassExpression>> {
        return self
            .all_of
            .as_ref()
            .map(|x| poly_containers::ListView::new(x));
    }
    fn slot_conditions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::SlotDefinition>> {
        return self
            .slot_conditions
            .as_ref()
            .map(poly_containers::MapView::new);
    }
}
impl ClassExpression for crate::ClassDefinition {
    fn any_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousClassExpression>> {
        return self
            .any_of
            .as_ref()
            .map(|x| poly_containers::ListView::new(x));
    }
    fn exactly_one_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousClassExpression>> {
        return self
            .exactly_one_of
            .as_ref()
            .map(|x| poly_containers::ListView::new(x));
    }
    fn none_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousClassExpression>> {
        return self
            .none_of
            .as_ref()
            .map(|x| poly_containers::ListView::new(x));
    }
    fn all_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousClassExpression>> {
        return self
            .all_of
            .as_ref()
            .map(|x| poly_containers::ListView::new(x));
    }
    fn slot_conditions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::SlotDefinition>> {
        return self
            .slot_conditions
            .as_ref()
            .map(poly_containers::MapView::new);
    }
}

impl ClassExpression for crate::ClassExpressionOrSubtype {
    fn any_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousClassExpression>> {
        match self {
            ClassExpressionOrSubtype::AnonymousClassExpression(val) => {
                val.any_of().map(|x| x.to_any())
            }
            ClassExpressionOrSubtype::ClassDefinition(val) => val.any_of().map(|x| x.to_any()),
        }
    }
    fn exactly_one_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousClassExpression>> {
        match self {
            ClassExpressionOrSubtype::AnonymousClassExpression(val) => {
                val.exactly_one_of().map(|x| x.to_any())
            }
            ClassExpressionOrSubtype::ClassDefinition(val) => {
                val.exactly_one_of().map(|x| x.to_any())
            }
        }
    }
    fn none_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousClassExpression>> {
        match self {
            ClassExpressionOrSubtype::AnonymousClassExpression(val) => {
                val.none_of().map(|x| x.to_any())
            }
            ClassExpressionOrSubtype::ClassDefinition(val) => val.none_of().map(|x| x.to_any()),
        }
    }
    fn all_of<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousClassExpression>> {
        match self {
            ClassExpressionOrSubtype::AnonymousClassExpression(val) => {
                val.all_of().map(|x| x.to_any())
            }
            ClassExpressionOrSubtype::ClassDefinition(val) => val.all_of().map(|x| x.to_any()),
        }
    }
    fn slot_conditions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::SlotDefinition>> {
        match self {
            ClassExpressionOrSubtype::AnonymousClassExpression(val) => {
                val.slot_conditions().map(|x| x.to_any())
            }
            ClassExpressionOrSubtype::ClassDefinition(val) => {
                val.slot_conditions().map(|x| x.to_any())
            }
        }
    }
}

pub trait AnonymousClassExpression: AnonymousExpression + ClassExpression {
    fn is_a<'a>(&'a self) -> Option<&'a str>;
    // fn is_a_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_is_a<E>(&mut self, value: Option<&'a str>) where E: Into<String>;
}

impl AnonymousClassExpression for crate::AnonymousClassExpression {
    fn is_a<'a>(&'a self) -> Option<&'a str> {
        return self.is_a.as_deref();
    }
}

pub trait ClassDefinition: Definition + ClassExpression {
    fn slots<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>>;
    // fn slots_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, String>>;
    // fn set_slots<E>(&mut self, value: Option<&Vec<String>>) where E: Into<String>;

    fn slot_usage<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::SlotDefinition>>;
    // fn slot_usage_mut(&mut self) -> &mut Option<impl poly_containers::MapRef<'a, String,crate::SlotDefinition>>;
    // fn set_slot_usage<E>(&mut self, value: Option<&HashMap<String, E>>) where E: Into<SlotDefinition>;

    fn attributes<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::SlotDefinition>>;
    // fn attributes_mut(&mut self) -> &mut Option<impl poly_containers::MapRef<'a, String,crate::SlotDefinition>>;
    // fn set_attributes<E>(&mut self, value: Option<&HashMap<String, E>>) where E: Into<SlotDefinition>;

    fn class_uri<'a>(&'a self) -> Option<&'a crate::uriorcurie>;
    // fn class_uri_mut(&mut self) -> &mut Option<&'a crate::uriorcurie>;
    // fn set_class_uri(&mut self, value: Option<&'a uriorcurie>);

    fn subclass_of<'a>(&'a self) -> Option<&'a crate::uriorcurie>;
    // fn subclass_of_mut(&mut self) -> &mut Option<&'a crate::uriorcurie>;
    // fn set_subclass_of(&mut self, value: Option<&'a uriorcurie>);

    fn union_of<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>>;
    // fn union_of_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, String>>;
    // fn set_union_of<E>(&mut self, value: Option<&Vec<String>>) where E: Into<String>;

    fn defining_slots<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>>;
    // fn defining_slots_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, String>>;
    // fn set_defining_slots<E>(&mut self, value: Option<&Vec<String>>) where E: Into<String>;

    fn tree_root(&self) -> Option<bool>;
    // fn tree_root_mut(&mut self) -> &mut Option<bool>;
    // fn set_tree_root(&mut self, value: Option<bool>);

    fn unique_keys<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::UniqueKey>>;
    // fn unique_keys_mut(&mut self) -> &mut Option<impl poly_containers::MapRef<'a, String,crate::UniqueKey>>;
    // fn set_unique_keys<E>(&mut self, value: Option<&HashMap<String, E>>) where E: Into<UniqueKey>;

    fn rules<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::ClassRule>>;
    // fn rules_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, crate::ClassRule>>;
    // fn set_rules<E>(&mut self, value: Option<&Vec<E>>) where E: Into<ClassRule>;

    fn classification_rules<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousClassExpression>>;
    // fn classification_rules_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, crate::AnonymousClassExpression>>;
    // fn set_classification_rules<E>(&mut self, value: Option<&Vec<E>>) where E: Into<AnonymousClassExpression>;

    fn slot_names_unique(&self) -> Option<bool>;
    // fn slot_names_unique_mut(&mut self) -> &mut Option<bool>;
    // fn set_slot_names_unique(&mut self, value: Option<bool>);

    fn represents_relationship(&self) -> Option<bool>;
    // fn represents_relationship_mut(&mut self) -> &mut Option<bool>;
    // fn set_represents_relationship(&mut self, value: Option<bool>);

    fn disjoint_with<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>>;
    // fn disjoint_with_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, String>>;
    // fn set_disjoint_with<E>(&mut self, value: Option<&Vec<String>>) where E: Into<String>;

    fn children_are_mutually_disjoint(&self) -> Option<bool>;
    // fn children_are_mutually_disjoint_mut(&mut self) -> &mut Option<bool>;
    // fn set_children_are_mutually_disjoint(&mut self, value: Option<bool>);

    fn extra_slots<'a>(&'a self) -> Option<&'a crate::ExtraSlotsExpression>;
    // fn extra_slots_mut(&mut self) -> &mut Option<&'a crate::ExtraSlotsExpression>;
    // fn set_extra_slots<E>(&mut self, value: Option<E>) where E: Into<ExtraSlotsExpression>;

    fn alias<'a>(&'a self) -> Option<&'a str>;
    // fn alias_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_alias(&mut self, value: Option<&'a str>);
}

impl ClassDefinition for crate::ClassDefinition {
    fn slots<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.slots.as_ref();
    }
    fn slot_usage<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::SlotDefinition>> {
        return self.slot_usage.as_ref().map(poly_containers::MapView::new);
    }
    fn attributes<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::SlotDefinition>> {
        return self.attributes.as_ref().map(poly_containers::MapView::new);
    }
    fn class_uri<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.class_uri.as_ref();
    }
    fn subclass_of<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.subclass_of.as_ref();
    }
    fn union_of<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.union_of.as_ref();
    }
    fn defining_slots<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.defining_slots.as_ref();
    }
    fn tree_root(&self) -> Option<bool> {
        return self.tree_root;
    }
    fn unique_keys<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::UniqueKey>> {
        return self.unique_keys.as_ref().map(poly_containers::MapView::new);
    }
    fn rules<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::ClassRule>> {
        return self
            .rules
            .as_ref()
            .map(|x| poly_containers::ListView::new(x));
    }
    fn classification_rules<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::AnonymousClassExpression>> {
        return self
            .classification_rules
            .as_ref()
            .map(|x| poly_containers::ListView::new(x));
    }
    fn slot_names_unique(&self) -> Option<bool> {
        return self.slot_names_unique;
    }
    fn represents_relationship(&self) -> Option<bool> {
        return self.represents_relationship;
    }
    fn disjoint_with<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.disjoint_with.as_ref();
    }
    fn children_are_mutually_disjoint(&self) -> Option<bool> {
        return self.children_are_mutually_disjoint;
    }
    fn extra_slots<'a>(&'a self) -> Option<&'a crate::ExtraSlotsExpression> {
        return self.extra_slots.as_deref();
    }
    fn alias<'a>(&'a self) -> Option<&'a str> {
        return self.alias.as_deref();
    }
}

pub trait ClassLevelRule {}

impl ClassLevelRule for crate::ClassLevelRule {}
impl ClassLevelRule for crate::ClassRule {}

impl ClassLevelRule for crate::ClassLevelRuleOrSubtype {}

pub trait ClassRule: ClassLevelRule + Extensible + Annotatable + CommonMetadata {
    fn preconditions<'a>(&'a self) -> Option<&'a crate::AnonymousClassExpression>;
    // fn preconditions_mut(&mut self) -> &mut Option<&'a crate::AnonymousClassExpression>;
    // fn set_preconditions<E>(&mut self, value: Option<E>) where E: Into<AnonymousClassExpression>;

    fn postconditions<'a>(&'a self) -> Option<&'a crate::AnonymousClassExpression>;
    // fn postconditions_mut(&mut self) -> &mut Option<&'a crate::AnonymousClassExpression>;
    // fn set_postconditions<E>(&mut self, value: Option<E>) where E: Into<AnonymousClassExpression>;

    fn elseconditions<'a>(&'a self) -> Option<&'a crate::AnonymousClassExpression>;
    // fn elseconditions_mut(&mut self) -> &mut Option<&'a crate::AnonymousClassExpression>;
    // fn set_elseconditions<E>(&mut self, value: Option<E>) where E: Into<AnonymousClassExpression>;

    fn bidirectional(&self) -> Option<bool>;
    // fn bidirectional_mut(&mut self) -> &mut Option<bool>;
    // fn set_bidirectional(&mut self, value: Option<bool>);

    fn open_world(&self) -> Option<bool>;
    // fn open_world_mut(&mut self) -> &mut Option<bool>;
    // fn set_open_world(&mut self, value: Option<bool>);

    fn deactivated(&self) -> Option<bool>;
    // fn deactivated_mut(&mut self) -> &mut Option<bool>;
    // fn set_deactivated(&mut self, value: Option<bool>);
}

impl ClassRule for crate::ClassRule {
    fn preconditions<'a>(&'a self) -> Option<&'a crate::AnonymousClassExpression> {
        return self.preconditions.as_deref();
    }
    fn postconditions<'a>(&'a self) -> Option<&'a crate::AnonymousClassExpression> {
        return self.postconditions.as_deref();
    }
    fn elseconditions<'a>(&'a self) -> Option<&'a crate::AnonymousClassExpression> {
        return self.elseconditions.as_deref();
    }
    fn bidirectional(&self) -> Option<bool> {
        return self.bidirectional;
    }
    fn open_world(&self) -> Option<bool> {
        return self.open_world;
    }
    fn deactivated(&self) -> Option<bool> {
        return self.deactivated;
    }
}

pub trait ArrayExpression: Extensible + Annotatable + CommonMetadata {
    fn exact_number_dimensions(&self) -> Option<isize>;
    // fn exact_number_dimensions_mut(&mut self) -> &mut Option<isize>;
    // fn set_exact_number_dimensions(&mut self, value: Option<isize>);

    fn minimum_number_dimensions(&self) -> Option<isize>;
    // fn minimum_number_dimensions_mut(&mut self) -> &mut Option<isize>;
    // fn set_minimum_number_dimensions(&mut self, value: Option<isize>);

    fn maximum_number_dimensions(
        &self,
    ) -> Option<array_expression_utl::maximum_number_dimensions_range>;
    // fn maximum_number_dimensions_mut(&mut self) -> &mut Option<array_expression_utl::maximum_number_dimensions_range>;
    // fn set_maximum_number_dimensions(&mut self, value: Option<&'a array_expression_utl::maximum_number_dimensions_range>);

    fn dimensions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::DimensionExpression>>;
    // fn dimensions_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, crate::DimensionExpression>>;
    // fn set_dimensions<E>(&mut self, value: Option<&Vec<E>>) where E: Into<DimensionExpression>;
}

impl ArrayExpression for crate::ArrayExpression {
    fn exact_number_dimensions(&self) -> Option<isize> {
        return self.exact_number_dimensions;
    }
    fn minimum_number_dimensions(&self) -> Option<isize> {
        return self.minimum_number_dimensions;
    }
    fn maximum_number_dimensions(
        &self,
    ) -> Option<array_expression_utl::maximum_number_dimensions_range> {
        self.maximum_number_dimensions.as_ref().cloned()
    }
    fn dimensions<'a>(
        &'a self,
    ) -> Option<impl poly_containers::SeqRef<'a, crate::DimensionExpression>> {
        return self.dimensions.as_ref();
    }
}

pub trait DimensionExpression: Extensible + Annotatable + CommonMetadata {
    fn alias<'a>(&'a self) -> Option<&'a str>;
    // fn alias_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_alias(&mut self, value: Option<&'a str>);

    fn maximum_cardinality(&self) -> Option<isize>;
    // fn maximum_cardinality_mut(&mut self) -> &mut Option<isize>;
    // fn set_maximum_cardinality(&mut self, value: Option<isize>);

    fn minimum_cardinality(&self) -> Option<isize>;
    // fn minimum_cardinality_mut(&mut self) -> &mut Option<isize>;
    // fn set_minimum_cardinality(&mut self, value: Option<isize>);

    fn exact_cardinality(&self) -> Option<isize>;
    // fn exact_cardinality_mut(&mut self) -> &mut Option<isize>;
    // fn set_exact_cardinality(&mut self, value: Option<isize>);
}

impl DimensionExpression for crate::DimensionExpression {
    fn alias<'a>(&'a self) -> Option<&'a str> {
        return self.alias.as_deref();
    }
    fn maximum_cardinality(&self) -> Option<isize> {
        return self.maximum_cardinality;
    }
    fn minimum_cardinality(&self) -> Option<isize> {
        return self.minimum_cardinality;
    }
    fn exact_cardinality(&self) -> Option<isize> {
        return self.exact_cardinality;
    }
}

pub trait PatternExpression: Extensible + Annotatable + CommonMetadata {
    fn syntax<'a>(&'a self) -> Option<&'a str>;
    // fn syntax_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_syntax(&mut self, value: Option<&'a str>);

    fn interpolated(&self) -> Option<bool>;
    // fn interpolated_mut(&mut self) -> &mut Option<bool>;
    // fn set_interpolated(&mut self, value: Option<bool>);

    fn partial_match(&self) -> Option<bool>;
    // fn partial_match_mut(&mut self) -> &mut Option<bool>;
    // fn set_partial_match(&mut self, value: Option<bool>);
}

impl PatternExpression for crate::PatternExpression {
    fn syntax<'a>(&'a self) -> Option<&'a str> {
        return self.syntax.as_deref();
    }
    fn interpolated(&self) -> Option<bool> {
        return self.interpolated;
    }
    fn partial_match(&self) -> Option<bool> {
        return self.partial_match;
    }
}

pub trait ImportExpression: Extensible + Annotatable + CommonMetadata {
    fn import_from<'a>(&'a self) -> &'a crate::uriorcurie;
    // fn import_from_mut(&mut self) -> &mut &'a crate::uriorcurie;
    // fn set_import_from(&mut self, value: uriorcurie);

    fn import_as<'a>(&'a self) -> Option<&'a crate::ncname>;
    // fn import_as_mut(&mut self) -> &mut Option<&'a crate::ncname>;
    // fn set_import_as(&mut self, value: Option<&'a ncname>);

    fn import_map<'a>(&'a self)
        -> Option<impl poly_containers::MapRef<'a, String, crate::Setting>>;
    // fn import_map_mut(&mut self) -> &mut Option<impl poly_containers::MapRef<'a, String,crate::Setting>>;
    // fn set_import_map<E>(&mut self, value: Option<&HashMap<String, E>>) where E: Into<Setting>;
}

impl ImportExpression for crate::ImportExpression {
    fn import_from<'a>(&'a self) -> &'a crate::uriorcurie {
        return &self.import_from;
    }
    fn import_as<'a>(&'a self) -> Option<&'a crate::ncname> {
        return self.import_as.as_ref();
    }
    fn import_map<'a>(
        &'a self,
    ) -> Option<impl poly_containers::MapRef<'a, String, crate::Setting>> {
        return self.import_map.as_ref();
    }
}

pub trait Setting {
    fn setting_key<'a>(&'a self) -> &'a crate::ncname;
    // fn setting_key_mut(&mut self) -> &mut &'a crate::ncname;
    // fn set_setting_key(&mut self, value: ncname);

    fn setting_value<'a>(&'a self) -> &'a str;
    // fn setting_value_mut(&mut self) -> &mut &'a str;
    // fn set_setting_value(&mut self, value: String);
}

impl Setting for crate::Setting {
    fn setting_key<'a>(&'a self) -> &'a crate::ncname {
        return &self.setting_key;
    }
    fn setting_value<'a>(&'a self) -> &'a str {
        return &self.setting_value[..];
    }
}

pub trait Prefix {
    fn prefix_prefix<'a>(&'a self) -> &'a crate::ncname;
    // fn prefix_prefix_mut(&mut self) -> &mut &'a crate::ncname;
    // fn set_prefix_prefix(&mut self, value: ncname);

    fn prefix_reference<'a>(&'a self) -> &'a crate::uri;
    // fn prefix_reference_mut(&mut self) -> &mut &'a crate::uri;
    // fn set_prefix_reference(&mut self, value: uri);
}

impl Prefix for crate::Prefix {
    fn prefix_prefix<'a>(&'a self) -> &'a crate::ncname {
        return &self.prefix_prefix;
    }
    fn prefix_reference<'a>(&'a self) -> &'a crate::uri {
        return &self.prefix_reference;
    }
}

pub trait LocalName {
    fn local_name_source<'a>(&'a self) -> &'a crate::ncname;
    // fn local_name_source_mut(&mut self) -> &mut &'a crate::ncname;
    // fn set_local_name_source(&mut self, value: ncname);

    fn local_name_value<'a>(&'a self) -> &'a str;
    // fn local_name_value_mut(&mut self) -> &mut &'a str;
    // fn set_local_name_value(&mut self, value: String);
}

impl LocalName for crate::LocalName {
    fn local_name_source<'a>(&'a self) -> &'a crate::ncname {
        return &self.local_name_source;
    }
    fn local_name_value<'a>(&'a self) -> &'a str {
        return &self.local_name_value[..];
    }
}

pub trait Example {
    fn value<'a>(&'a self) -> Option<&'a str>;
    // fn value_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_value(&mut self, value: Option<&'a str>);

    fn value_description<'a>(&'a self) -> Option<&'a str>;
    // fn value_description_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_value_description(&mut self, value: Option<&'a str>);

    fn value_object<'a>(&'a self) -> Option<&'a crate::Anything>;
    // fn value_object_mut(&mut self) -> &mut Option<&'a crate::Anything>;
    // fn set_value_object<E>(&mut self, value: Option<E>) where E: Into<Anything>;
}

impl Example for crate::Example {
    fn value<'a>(&'a self) -> Option<&'a str> {
        return self.value.as_deref();
    }
    fn value_description<'a>(&'a self) -> Option<&'a str> {
        return self.value_description.as_deref();
    }
    fn value_object<'a>(&'a self) -> Option<&'a crate::Anything> {
        return self.value_object.as_ref();
    }
}

pub trait AltDescription {
    fn alt_description_source<'a>(&'a self) -> &'a str;
    // fn alt_description_source_mut(&mut self) -> &mut &'a str;
    // fn set_alt_description_source(&mut self, value: String);

    fn alt_description_text<'a>(&'a self) -> &'a str;
    // fn alt_description_text_mut(&mut self) -> &mut &'a str;
    // fn set_alt_description_text(&mut self, value: String);
}

impl AltDescription for crate::AltDescription {
    fn alt_description_source<'a>(&'a self) -> &'a str {
        return &self.alt_description_source[..];
    }
    fn alt_description_text<'a>(&'a self) -> &'a str {
        return &self.alt_description_text[..];
    }
}

pub trait PermissibleValue: Extensible + Annotatable + CommonMetadata {
    fn text<'a>(&'a self) -> &'a str;
    // fn text_mut(&mut self) -> &mut &'a str;
    // fn set_text(&mut self, value: String);

    fn meaning<'a>(&'a self) -> Option<&'a crate::uriorcurie>;
    // fn meaning_mut(&mut self) -> &mut Option<&'a crate::uriorcurie>;
    // fn set_meaning(&mut self, value: Option<&'a uriorcurie>);

    fn unit<'a>(&'a self) -> Option<&'a crate::UnitOfMeasure>;
    // fn unit_mut(&mut self) -> &mut Option<&'a crate::UnitOfMeasure>;
    // fn set_unit<E>(&mut self, value: Option<E>) where E: Into<UnitOfMeasure>;

    fn instantiates<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>>;
    // fn instantiates_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>>;
    // fn set_instantiates(&mut self, value: Option<&Vec<uriorcurie>>);

    fn implements<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>>;
    // fn implements_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>>;
    // fn set_implements(&mut self, value: Option<&Vec<uriorcurie>>);

    fn is_a<'a>(&'a self) -> Option<&'a str>;
    // fn is_a_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_is_a<E>(&mut self, value: Option<&'a str>) where E: Into<String>;

    fn mixins<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>>;
    // fn mixins_mut(&mut self) -> &mut Option<impl poly_containers::SeqRef<'a, String>>;
    // fn set_mixins<E>(&mut self, value: Option<&Vec<String>>) where E: Into<String>;
}

impl PermissibleValue for crate::PermissibleValue {
    fn text<'a>(&'a self) -> &'a str {
        return &self.text[..];
    }
    fn meaning<'a>(&'a self) -> Option<&'a crate::uriorcurie> {
        return self.meaning.as_ref();
    }
    fn unit<'a>(&'a self) -> Option<&'a crate::UnitOfMeasure> {
        return self.unit.as_ref();
    }
    fn instantiates<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.instantiates.as_ref();
    }
    fn implements<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, crate::uriorcurie>> {
        return self.implements.as_ref();
    }
    fn is_a<'a>(&'a self) -> Option<&'a str> {
        return self.is_a.as_deref();
    }
    fn mixins<'a>(&'a self) -> Option<impl poly_containers::SeqRef<'a, String>> {
        return self.mixins.as_ref();
    }
}

pub trait UniqueKey: Extensible + Annotatable + CommonMetadata {
    fn unique_key_name<'a>(&'a self) -> &'a str;
    // fn unique_key_name_mut(&mut self) -> &mut &'a str;
    // fn set_unique_key_name(&mut self, value: String);

    fn unique_key_slots<'a>(&'a self) -> impl poly_containers::SeqRef<'a, String>;
    // fn unique_key_slots_mut(&mut self) -> &mut impl poly_containers::SeqRef<'a, String>;
    // fn set_unique_key_slots<E>(&mut self, value: &Vec<String>) where E: Into<String>;

    fn consider_nulls_inequal(&self) -> Option<bool>;
    // fn consider_nulls_inequal_mut(&mut self) -> &mut Option<bool>;
    // fn set_consider_nulls_inequal(&mut self, value: Option<bool>);
}

impl UniqueKey for crate::UniqueKey {
    fn unique_key_name<'a>(&'a self) -> &'a str {
        return &self.unique_key_name[..];
    }
    fn unique_key_slots<'a>(&'a self) -> impl poly_containers::SeqRef<'a, String> {
        return &self.unique_key_slots;
    }
    fn consider_nulls_inequal(&self) -> Option<bool> {
        return self.consider_nulls_inequal;
    }
}

pub trait TypeMapping: Extensible + Annotatable + CommonMetadata {
    fn framework_key<'a>(&'a self) -> &'a str;
    // fn framework_key_mut(&mut self) -> &mut &'a str;
    // fn set_framework_key(&mut self, value: String);

    fn mapped_type<'a>(&'a self) -> Option<&'a str>;
    // fn mapped_type_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_mapped_type<E>(&mut self, value: Option<&'a str>) where E: Into<String>;

    fn string_serialization<'a>(&'a self) -> Option<&'a str>;
    // fn string_serialization_mut(&mut self) -> &mut Option<&'a str>;
    // fn set_string_serialization(&mut self, value: Option<&'a str>);
}

impl TypeMapping for crate::TypeMapping {
    fn framework_key<'a>(&'a self) -> &'a str {
        return &self.framework_key[..];
    }
    fn mapped_type<'a>(&'a self) -> Option<&'a str> {
        return self.mapped_type.as_deref();
    }
    fn string_serialization<'a>(&'a self) -> Option<&'a str> {
        return self.string_serialization.as_deref();
    }
}

pub trait ExtraSlotsExpression: Expression {
    fn allowed(&self) -> Option<bool>;
    // fn allowed_mut(&mut self) -> &mut Option<bool>;
    // fn set_allowed(&mut self, value: Option<bool>);

    fn range_expression<'a>(&'a self) -> Option<&'a crate::AnonymousSlotExpression>;
    // fn range_expression_mut(&mut self) -> &mut Option<&'a crate::AnonymousSlotExpression>;
    // fn set_range_expression<E>(&mut self, value: Option<E>) where E: Into<AnonymousSlotExpression>;
}

impl ExtraSlotsExpression for crate::ExtraSlotsExpression {
    fn allowed(&self) -> Option<bool> {
        return self.allowed;
    }
    fn range_expression<'a>(&'a self) -> Option<&'a crate::AnonymousSlotExpression> {
        return self.range_expression.as_deref();
    }
}
