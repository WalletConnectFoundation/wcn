use {
    core::panic,
    proc_macro::TokenStream,
    quote::{format_ident, quote},
    syn::{
        Attribute,
        Data,
        DeriveInput,
        Field,
        GenericParam,
        Ident,
        Lifetime,
        Type,
        parse_macro_input,
        visit_mut::VisitMut,
    },
};

#[proc_macro_derive(Message)]
pub fn derive_message(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let type_name = input.ident;

    let has_lifetime = input
        .generics
        .params
        .iter()
        .any(|param| matches!(param, GenericParam::Lifetime(_)));

    if !has_lifetime {
        return TokenStream::from(quote! {
            impl wcn_rpc::BorrowedMessage for #type_name {
                type Owned = #type_name;

                fn into_owned(self) -> Self::Owned {
                    self
                }
            }
        });
    }

    let type_name_str = type_name.to_string();

    let syn::Data::Struct(syn::DataStruct {
        fields: syn::Fields::Named(fields),
        ..
    }) = input.data
    else {
        panic!("Only named structs are supported");
    };

    let fields: Vec<_> = fields.named.into_iter().collect();

    let owned_type_name = type_name_str
        .strip_suffix("Borrowed")
        .map(|s| format_ident!("{s}"))
        .unwrap_or_else(|| format_ident!("{type_name}Owned"));

    let owned_fields = fields.iter().map(|field| {
        let field_name = &field.ident;
        let field_type = replace_named_lifetimes_with_static(field.ty.clone());

        quote! {
            pub #field_name: <#field_type as wcn_rpc::BorrowedMessage>::Owned
        }
    });

    let into_owned = fields.iter().map(|field| {
        let field_name = &field.ident;
        quote! { #field_name: wcn_rpc::BorrowedMessage::into_owned(self.#field_name) }
    });

    TokenStream::from(quote! {
        #[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, wcn_rpc::Message)]
        pub struct #owned_type_name {
            #(#owned_fields),*
        }

        impl<'a> wcn_rpc::BorrowedMessage for #type_name<'a> {
            type Owned = #owned_type_name;

            fn into_owned(self) -> Self::Owned {
                #owned_type_name {
                    #(#into_owned),*
                }
            }
        }
    })
}

fn replace_named_lifetimes_with_static(mut ty: Type) -> Type {
    struct LifetimeReplacer;

    impl VisitMut for LifetimeReplacer {
        fn visit_lifetime_mut(&mut self, lt: &mut Lifetime) {
            *lt = Lifetime::new("'static", lt.span());
        }
    }

    let mut replacer = LifetimeReplacer;
    replacer.visit_type_mut(&mut ty);
    ty
}

// #[proc_macro_derive(BorrowedMessage, attributes(message))]
fn derive_borrowed_message_(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let type_name = input.ident.to_string();

    let owned_type_name = type_name
        .strip_suffix("Borrowed")
        .expect("Type name must have `*Borrowed` suffix");

    match message_kind(&input.attrs, input.data) {
        MessageKind::Clone => {
            return TokenStream::from(quote! {
                impl wcn_rpc::MessageV2 for #owned_type_name {
                    type Borrowed<'a> = &'a Self;

                    fn borrow(&self) -> Self::Borrowed<'_> {
                        &self
                    }
                }

                impl wcn_rpc::BorrowedMessage for &#owned_type_name {
                    type Owned = #owned_type_name;

                    fn into_owned(self) -> Self::Owned {
                        self.clone()
                    }
                }
            });
        }
        MessageKind::Copy => {
            return TokenStream::from(quote! {
                impl wcn_rpc::MessageV2 for #owned_type_name {
                    type Borrowed<'a> = Self;

                    fn borrow(&self) -> Self::Borrowed<'_> {
                        *self
                    }
                }

                impl wcn_rpc::BorrowedMessage for #owned_type_name {
                    type Owned = Self;

                    fn into_owned(self) -> Self::Owned {
                        self
                    }
                }
            });
        }
        MessageKind::Borrow { fields } => {
            let borrowed_type_ident = format_ident!("{}Borrowed", owned_type_name);

            let borrowed_fields = fields.iter().map(|field| {
                let field_name = &field.ident;
                let field_type = &field.ty;
                quote! {
                    pub #field_name: <#field_type as wcn_rpc::MessageV2>::Borrowed<'a>
                }
            });

            let borrow = fields.iter().map(|field| {
                let field_name = &field.ident;
                quote! { #field_name: wcn_rpc::MessageV2::borrow(&self.#field_name) }
            });

            let into_owned = fields.iter().map(|field| {
                let field_name = &field.ident;
                quote! { #field_name: wcn_rpc::BorrowedMessage::into_owned(self.#field_name) }
            });

            TokenStream::from(quote! {
                #[derive(Clone, Debug, PartialEq, Eq)]
                pub struct #borrowed_type_ident<'a> {
                    #(#borrowed_fields),*
                }

                impl wcn_rpc::MessageV2 for #owned_type_name {
                    type Borrowed<'a> = #borrowed_type_ident<'a>;

                    fn borrow(&self) -> Self::Borrowed<'_> {
                        #borrowed_type_ident {
                            #(#borrow),*
                        }
                    }
                }

                impl<'a> wcn_rpc::BorrowedMessage for #borrowed_type_ident<'a> {
                    type Owned = #owned_type_name;

                    fn into_owned(self) -> Self::Owned {
                        #owned_type_name {
                            #(#into_owned),*
                        }
                    }
                }
            })
        }
    }
}

enum MessageKind {
    Clone,
    Copy,
    Borrow { fields: Vec<Field> },
}

fn message_kind(attrs: &[Attribute], data: Data) -> MessageKind {
    let attrs: Vec<_> = attrs
        .iter()
        .filter(|attr| attr.path().is_ident("message"))
        .cloned()
        .collect();

    if attrs.is_empty() {
        return MessageKind::Clone;
    }

    if attrs.len() > 1 {
        panic!("There should be no more than one #[message(..)] attribute");
    }

    let ident: Ident = attrs[0].meta.require_list().unwrap().parse_args().unwrap();

    match ident.to_string().as_str() {
        "copy" => MessageKind::Copy,
        "borrow" => {
            let syn::Data::Struct(syn::DataStruct {
                fields: syn::Fields::Named(fields),
                ..
            }) = data
            else {
                panic!("Only named structs are supported");
            };

            MessageKind::Borrow {
                fields: fields.named.into_iter().collect(),
            }
        }
        other => panic!(
            "Unexpected attribute `{other}`\nShould be either
    `copy` or `borrow`"
        ),
    }
}
