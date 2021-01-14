/// Compatibility layer between actix v2 and paperclip
pub mod actix {
    /// Expose macros to create resource handlers, allowing multiple HTTP
    /// method guards.
    pub use actix_openapi_macros::*;

    /// Factory type which helps convert actix_web::dev::HttpServiceFactory into
    /// actix_web::dev::HttpServiceFactory + paperclip::actix::Mountable
    /// which is required by the paperclip `service` function.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use mayastor_macros::actix::{get, Factory};
    /// # use paperclip::actix::Mountable
    /// # use actix_web::dev::HttpServiceFactory
    /// pub(crate) fn factory() -> impl Mountable + HttpServiceFactory {
    ///     Factory::new().service(example1).service(example2)
    /// }
    /// #[get("/example1")]
    /// async fn example1() -> Json<()> {
    ///     Json(())
    /// }
    /// #[get("/example2")]
    /// async fn example2() -> Json<()> {
    ///     Json(())
    /// }
    #[derive(Default)]
    pub struct Factory {
        path_map: std::collections::BTreeMap<
            String,
            paperclip::v2::models::DefaultPathItemRaw,
        >,
        definitions: std::collections::BTreeMap<
            String,
            paperclip::v2::models::DefaultSchemaRaw,
        >,
        security: std::collections::BTreeMap<
            String,
            paperclip::v2::models::SecurityScheme,
        >,
        resources: Vec<paperclip::actix::web::Resource>,
    }

    impl Factory {
        /// Create empty factory
        pub fn new() -> Self {
            Self::default()
        }

        /// Register Http Service a la
        /// [`actix_web::App::service`](https://docs.rs/actix-web/*/actix_web/struct.App.html#method.service).
        pub fn service<F>(mut self, mut factory: F) -> Self
        where
            F: paperclip::actix::Mountable
                + actix_web::dev::HttpServiceFactory
                + Resource,
        {
            self.update_from_mountable(&mut factory);
            self.resources.push(factory.resource());
            self
        }

        fn update_from_mountable<M>(&mut self, factory: &mut M)
        where
            M: paperclip::actix::Mountable,
        {
            self.definitions.extend(factory.definitions().into_iter());
            factory.update_operations(&mut self.path_map);

            paperclip::v2::models::SecurityScheme::append_map(
                factory.security_definitions(),
                &mut self.security,
            );
        }
    }

    impl actix_web::dev::HttpServiceFactory for Factory {
        fn register(self, config: &mut actix_web::dev::AppService) {
            for resource in self.resources {
                resource.register(config);
            }
        }
    }

    impl paperclip::actix::Mountable for Factory {
        fn path(&self) -> &str {
            unimplemented!(
                "Factory has multiple paths. Use `update_operations` object instead."
            );
        }

        fn operations(
            &mut self,
        ) -> std::collections::BTreeMap<
            paperclip::v2::models::HttpMethod,
            paperclip::v2::models::DefaultOperationRaw,
        > {
            unimplemented!(
                "Factory has multiple paths. Use `update_operations` object instead."
            );
        }

        fn definitions(
            &mut self,
        ) -> std::collections::BTreeMap<
            String,
            paperclip::v2::models::DefaultSchemaRaw,
        > {
            self.definitions.clone()
        }

        fn security_definitions(
            &mut self,
        ) -> std::collections::BTreeMap<
            String,
            paperclip::v2::models::SecurityScheme,
        > {
            self.security.clone()
        }

        fn update_operations(
            &mut self,
            map: &mut std::collections::BTreeMap<
                String,
                paperclip::v2::models::DefaultPathItemRaw,
            >,
        ) {
            for (path, item) in std::mem::replace(
                &mut self.path_map,
                std::collections::BTreeMap::new(),
            ) {
                let op_map = map.entry(path).or_insert_with(Default::default);
                op_map.methods.extend(item.methods.into_iter());
            }
        }
    }

    /// Http Service handlers expose a `actix::web::Resource`
    pub trait Resource {
        fn resource(&self) -> paperclip::actix::web::Resource;
    }
}
